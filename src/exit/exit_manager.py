import asyncio
import logging
import time
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class ExitManager:
    """Hybrid exit: tiered + ATR trailing + ATR stop + time exit.
    Quant-bot inspired: atr_stop/atr_target from quantbot/mathlib/risk.py,
    halt logic from risk.py, stats from stats.py.
    Memes: stops are % or ATR whichever tighter; trailing is ATR-based not fixed 20%.
    """
    def __init__(self, config: dict, executor, database):
        self.config = config
        self.executor = executor
        self.db = database
        self.paper_mode = config.get('execution', {}).get('paper_mode', True)

        self.tiers = self.config.get('tiers', [
            {'multiplier': 2, 'sell_percent': 25},
            {'multiplier': 5, 'sell_percent': 25},
            {'multiplier': 10, 'sell_percent': 25}
        ])

        trailing = self.config.get('trailing_stop', {})
        self.trailing_enabled = trailing.get('enabled', True)
        self.trailing_callback = trailing.get('callback_percent', 20)
        self.min_profit_trailing = trailing.get('min_profit_before_trailing', 50)
        self.atr_trailing_mult = trailing.get('atr_trailing_mult', 1.8)  # NEW: ATR trailing

        time_exit = self.config.get('time_exit', {})
        self.time_exit_enabled = time_exit.get('enabled', True)
        self.max_hold_hours = time_exit.get('max_hold_hours', 24)
        self.emergency_loss = time_exit.get('emergency_exit_loss_percent', 50)
        self.check_interval = time_exit.get('check_interval_seconds', 10)

        # ATR stop config (from quant-bot)
        risk_cfg = config.get('risk', {})
        self.atr_stop_mult = risk_cfg.get('atr_stop_mult', 1.8)  # meme tighter than quant 2.5
        self.atr_period = risk_cfg.get('atr_period', 14)

        self.running = False
        self.position_states: Dict[int, dict] = {}
        self.memory = None
        self.tg_bot = None  # push updates to TG
        self.demo_wallet = None  # demo wallet for honest paper fills (slippage+fee)

    async def start(self, chains_config: dict):
        self.running = True
        self.chains_config = chains_config
        logger.info(f"Exit manager started (ATR stop {self.atr_stop_mult}x, trailing {self.atr_trailing_mult}x ATR or {self.trailing_callback}% callback)")
        while self.running:
            try:
                await self._check_all_positions()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Exit check error: {e}")
                await asyncio.sleep(5)

    async def stop(self):
        self.running = False

    async def register_position(self, position_id: int, entry_price: float,
                                token_address: str, chain: str, token_amount: float,
                                atr: float = 0, stop_price: float = None, entry_mcap: float = 0, buy_amount: float = 0):
        # Compute initial ATR stop if not provided
        if stop_price is None:
            try:
                from ..math.risk import atr_stop
            except ImportError:
                try:
                    from math.risk import atr_stop
                except:
                    def atr_stop(ep, side, atr_v, mult=1.8): return ep * 0.70
            if atr and atr > 0:
                stop_price = atr_stop(entry_price, 1, atr, self.atr_stop_mult)
            else:
                stop_price = entry_price * 0.65  # 35% stop for memes if no ATR yet

        self.position_states[position_id] = {
            'token_address': token_address,
            'chain': chain,
            'entry_price': entry_price,
            'token_amount': token_amount,
            'current_price': entry_price,
            'peak_price': entry_price,
            'atr': atr,
            'stop_price': stop_price,
            'entry_mcap': entry_mcap,
            'buy_amount': buy_amount,
            'price_history': [entry_price],
            'highest_tier_reached': 0,
            'tiers_executed': [],
            'remaining_percent': 100,
            'realized_usd': 0.0,  # net proceeds accumulated across partial sells (all-in)
            'registered_at': time.time(),
            'last_update': time.time()
        }
        logger.info(f"Registered pos {position_id}: {token_address[:8]}... entry ${entry_price:.8f} mcap ${entry_mcap:.0f} stop ${stop_price:.8f} ATR ${atr:.8f} buy ${buy_amount:.2f}")

    async def _check_all_positions(self):
        positions = self.db.get_open_positions()
        for position in positions:
            pos_id = position['id']
            if pos_id not in self.position_states:
                await self.register_position(pos_id, position['entry_price'], position['token_address'], position['chain'], position['token_amount'], buy_amount=position.get('entry_amount_usd',0))
            state = self.position_states[pos_id]
            current_price = await self.executor.get_token_price(position['token_address'], position['chain'], self.chains_config)
            if current_price <= 0:
                continue
            state['current_price'] = current_price
            state['last_update'] = time.time()
            if self.paper_mode and self.demo_wallet:
                self.demo_wallet.mark_holding(position['token_address'], current_price)
            state['price_history'].append(current_price)
            if len(state['price_history']) > 200:
                state['price_history'] = state['price_history'][-200:]
            # Update ATR dynamically
            try:
                from ..math.risk import compute_atr
            except:
                try:
                    from math.risk import compute_atr
                except:
                    compute_atr = lambda p, period=14: 0
            new_atr = compute_atr(state['price_history'], self.atr_period)
            if new_atr > 0:
                state['atr'] = new_atr
            if current_price > state['peak_price']:
                state['peak_price'] = current_price
                # Ratchet stop up (never down) - quant-bot trailing logic
                if state['atr'] and state['atr'] > 0:
                    try:
                        from ..math.risk import atr_stop
                    except:
                        try:
                            from math.risk import atr_stop
                        except:
                            atr_stop = lambda ep, s, a, m: ep*0.70
                    new_stop = current_price - state['atr'] * self.atr_trailing_mult
                    if new_stop > state['stop_price']:
                        state['stop_price'] = new_stop
            pnl_percent = ((current_price - state['entry_price']) / state['entry_price']) * 100 if state['entry_price']>0 else 0
            exit_result = await self._check_exit_conditions(pos_id, state, pnl_percent, position)
            if exit_result:
                await self._execute_exit(pos_id, state, exit_result, position)

    async def _check_exit_conditions(self, pos_id: int, state: dict, pnl_percent: float, position: dict) -> Optional[dict]:
        # 1. ATR stop hit (hard stop - quant-bot style)
        if state['current_price'] <= state['stop_price']:
            return {'reason': 'atr_stop', 'sell_percent': state['remaining_percent'],
                    'details': f"ATR stop ${state['stop_price']:.8f} hit (entry ${state['entry_price']:.8f}, peak ${state['peak_price']:.8f})"}

        # 2. Time exit
        if self.time_exit_enabled:
            hours_held = (time.time() - state['registered_at']) / 3600
            if hours_held >= self.max_hold_hours:
                return {'reason': 'time_exit', 'sell_percent': state['remaining_percent'],
                        'details': f'Max hold {hours_held:.1f}h'}

        # 3. Emergency loss (kept, but ATR stop usually triggers first)
        if pnl_percent <= -self.emergency_loss:
            return {'reason': 'emergency_loss', 'sell_percent': state['remaining_percent'],
                    'details': f'Emergency loss {pnl_percent:.1f}%'}

        # 4. ATR trailing stop (after min profit) - more sensitive than fixed 20%
        if self.trailing_enabled and pnl_percent >= self.min_profit_trailing:
            # Two triggers: fixed % drawdown OR ATR breach from peak
            drawdown = ((state['peak_price'] - state['current_price']) / state['peak_price']) * 100 if state['peak_price']>0 else 0
            atr_breach = False
            if state['atr'] and state['atr'] > 0:
                atr_breach = state['current_price'] < (state['peak_price'] - state['atr'] * self.atr_trailing_mult)
            if drawdown >= self.trailing_callback or atr_breach:
                return {'reason': 'trailing_stop', 'sell_percent': state['remaining_percent'],
                        'details': f'Trailing: peak ${state["peak_price"]:.8f} -> ${state["current_price"]:.8f} drawdown {drawdown:.1f}% ATR {state["atr"]:.8f}'}

        # 5. Tiered exits
        for i, tier in enumerate(self.tiers):
            tier_num = i + 1
            if tier_num <= state['highest_tier_reached']:
                continue
            if pnl_percent >= ((tier['multiplier'] - 1) * 100):
                sell_percent = min(tier['sell_percent'], state['remaining_percent'])
                return {'reason': f'tier_{tier_num}', 'tier_multiplier': tier['multiplier'],
                        'sell_percent': sell_percent,
                        'details': f'Tier {tier_num} {tier["multiplier"]}x at {pnl_percent:.1f}%'}

        return None

    async def _execute_exit(self, pos_id: int, state: dict, exit_result: dict, position: dict):
        reason = exit_result['reason']
        sell_percent = exit_result['sell_percent']
        logger.info(f"Exit {pos_id}: {reason} sell {sell_percent}% - {exit_result.get('details','')}")
        try:
            if self.paper_mode:
                tokens_to_sell = state['token_amount'] * (sell_percent / 100.0)
                net_usd = 0.0
                if self.demo_wallet:
                    fill = self.demo_wallet.sell(state['token_address'], tokens_to_sell, state['current_price'], reason)
                    if fill:
                        net_usd = fill['net_usd']
                else:
                    net_usd = tokens_to_sell * state['current_price']
                state['realized_usd'] = state.get('realized_usd', 0.0) + net_usd
                result = {'success': True, 'paper': True, 'tx_hash': f"paper_{pos_id}_{reason}", 'net_usd': net_usd}
                logger.info(f"[PAPER] Sell {sell_percent}% pos {pos_id} {reason} - gross-to-net via demo wallet: ${net_usd:.4f} net (slip+fee included)")
            else:
                result = await self.executor.sell_token(state['token_address'], state['chain'], state['token_amount'], sell_percent, self.chains_config)
            if result and result.get('success'):
                state['remaining_percent'] -= sell_percent
                if reason.startswith('tier_'):
                    tier_num = int(reason.split('_')[1])
                    state['highest_tier_reached'] = tier_num
                    state['tiers_executed'].append({'tier': tier_num, 'multiplier': exit_result.get('tier_multiplier'), 'price': state['current_price'], 'percent_sold': sell_percent, 'net_usd': result.get('net_usd', 0), 'timestamp': datetime.now().isoformat()})
                    self.db.update_position_tier(pos_id, state['tiers_executed'])
                logger.info(f"Sold {sell_percent}% pos {pos_id} {state['token_address'][:8]}... ({reason}) remaining {state['remaining_percent']}%")
                try:
                    if self.tg_bot:
                        self.tg_bot.broadcast(f"💰 <b>SELL {sell_percent}%</b> {state['token_address'][:6]}... {state['chain']} {reason}\nPrice ${state['current_price']:.8f} remaining {state['remaining_percent']}% <code>{state['token_address']}</code>")
                except: pass
                if state['remaining_percent'] <= 0:
                    await self._close_position(pos_id, state, position, reason)
            else:
                logger.error(f"Exit failed {pos_id}: {result}")
        except Exception as e:
            logger.error(f"Exit exec error {pos_id}: {e}")

    async def _close_position(self, pos_id: int, state: dict, position: dict, reason: str):
        try:
            entry_value = float(position['entry_amount_usd'])  # all-in USD cost (fees included at buy)
            # Final sell of remaining tokens through the demo wallet for honest net proceeds
            remaining_amt = state['token_amount'] * (state['remaining_percent'] / 100.0)
            proceeds = float(state.get('realized_usd', 0.0))
            if remaining_amt > 1e-12:
                if self.paper_mode and self.demo_wallet:
                    ff = self.demo_wallet.sell(state['token_address'], remaining_amt, state['current_price'], f'close_{reason}')
                    net = ff['net_usd'] if ff else remaining_amt * state['current_price']
                else:
                    net = remaining_amt * state['current_price']  # live: filled estimate at market
                proceeds += net
            # ALL-IN honest return math: x = net proceeds / total cost (slip+fees already inside both)
            x = (proceeds / entry_value) if entry_value > 0 else 0.0
            roi_pct = (x - 1.0) * 100.0
            pnl = proceeds - entry_value
            self.db.close_position(pos_id, state['current_price'], proceeds, reason, state['tiers_executed'])
            # Update equity curve
            try:
                equity = self.db.get_current_equity()
                self.db.record_equity(equity)
            except: pass
            # Live mcap at exit for journal (not synthetic)
            exit_live = {}
            try:
                exit_live = await self.executor.get_token_live_data(state['token_address'], state['chain'], self.chains_config)
            except: pass
            exit_mcap = exit_live.get('mcap', 0) if exit_live else 0
            # Memory: record closed trade with LIVE maths (profit, x, ROI, mcap stamped)
            try:
                if self.memory:
                    hold_min = (time.time() - state['registered_at'])/60
                    # Use live exit price/mcap, not synthetic
                    feats = self.memory._featurize(state['token_address'], state['chain'], state['entry_price'],
                        liquidity=0, mcap=state.get('entry_mcap',0), scout_score=50)
                    self.memory.record_close(state['token_address'], state['chain'], state['entry_price'], state['current_price'], hold_min, feats,
                        entry_mcap=state.get('entry_mcap',0), exit_mcap=exit_mcap,
                        buy_amount=state.get('buy_amount', entry_value), token_amount=state.get('token_amount',0))
                    if self.memory.stats()['total_memories'] % 25 == 0 and self.memory.stats()['ready_for_transformer']:
                        logger.info(f"Memory {self.memory.stats()['total_memories']} trades -> ready for transformer training")
            except Exception as e:
                logger.debug(f"Memory record error: {e}")
            # ALL-IN profit maths on real net proceeds: x = proceeds/cost, ROI = (x-1)*100
            logger.info(f"Pos {pos_id} closed {reason} exit ${state['current_price']:.8f} mcap ${exit_mcap:.0f} | cost ${entry_value:.4f} proceeds ${proceeds:.4f} -> x{x:.4f} ROI {roi_pct:.2f}% pnl ${pnl:.4f}")
            del self.position_states[pos_id]
        except Exception as e:
            logger.error(f"Close error {pos_id}: {e}")

    def get_position_state(self, position_id: int) -> Optional[dict]:
        return self.position_states.get(position_id)

    def get_all_states(self) -> Dict[int, dict]:
        return self.position_states.copy()

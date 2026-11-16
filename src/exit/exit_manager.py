import asyncio
import json
import logging
import time
from decimal import Decimal
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class ExitManager:
    """Hybrid exit: tiered + ATR trailing + ATR stop + time exit.
    Quant-bot inspired: atr_stop/atr_target from quantbot/mathlib/risk.py,
    halt logic from risk.py, stats from stats.py.
    Memes: stops are % or ATR whichever tighter; trailing is ATR-based not fixed 20%.
    """

    def __init__(self, config: dict, executor, database, on_position_close=None):
        self.config = config
        self.executor = executor
        self.db = database
        self.paper_mode = config.get('execution', {}).get('paper_mode', True)
        self.demo_wallet = None
        self.on_position_close = on_position_close

        # Exit config
        exit_cfg = config.get('exit', {})
        self.tiers = exit_cfg.get('tiers', [
            {'multiplier': 2, 'sell_percent': 25},
            {'multiplier': 5, 'sell_percent': 25},
            {'multiplier': 10, 'sell_percent': 25},
        ])
        self.trailing_enabled = exit_cfg.get('trailing_stop', {}).get('enabled', True)
        self.trailing_callback = exit_cfg.get('trailing_stop', {}).get('callback_percent', 20)
        self.atr_trailing_mult = exit_cfg.get('trailing_stop', {}).get('atr_trailing_mult', 1.8)
        self.min_profit_trailing = exit_cfg.get('trailing_stop', {}).get('min_profit_before_trailing', 50)

        self.time_exit_enabled = exit_cfg.get('time_exit', {}).get('enabled', True)
        self.max_hold_hours = exit_cfg.get('time_exit', {}).get('max_hold_hours', 24)
        self.emergency_loss = exit_cfg.get('time_exit', {}).get('emergency_exit_loss_percent', 20)
        self.check_interval = exit_cfg.get('time_exit', {}).get('check_interval_seconds', 10)

        risk_cfg = config.get('risk', {})
        self.atr_stop_mult = risk_cfg.get('atr_stop_mult', 1.8)
        self.atr_period = risk_cfg.get('atr_period', 14)

        self.chains_config = config.get('chains', {})
        self.position_states = {}
        self.running = False

    async def start(self, chains_config: dict):
        self.chains_config = chains_config
        self.running = True
        # Rehydrate open positions from DB
        positions = self.db.get_open_positions()
        for position in positions:
            pos_id = position['id']
            await self.register_position(
                pos_id, position['entry_price'], position['token_address'],
                position['chain'], position['token_amount'],
                buy_amount=position.get('entry_amount_usd', 0),
                wallet_trade_id=position.get('wallet_trade_id'),
                token_amount_raw=position.get('token_amount_raw'),
                token_decimals=position.get('token_decimals'),
                entry_sol_lamports=position.get('entry_sol_lamports'),
                tier_exits=position.get('tier_exits'),
            )
        logger.info(f"Exit manager started (ATR stop {self.atr_stop_mult}x, trailing {self.atr_trailing_mult}x ATR or {self.trailing_callback}% callback)")
        while self.running:
            try:
                await self._check_all_positions()
            except Exception as e:
                logger.error(f"Exit check error: {e}")
            await asyncio.sleep(self.check_interval)

    async def stop(self):
        self.running = False
        self.position_states.clear()

    async def register_position(self, position_id: int, entry_price: float,
                                token_address: str, chain: str, token_amount: float,
                                atr: float = 0, stop_price: float = None, entry_mcap: float = 0, buy_amount: float = 0,
                                exit_profile: dict = None, wallet_trade_id: str = None,
                                token_amount_raw: int = None, token_decimals: int = None,
                                entry_sol_lamports: int = None, tier_exits: list = None):
        try:
            tier_exits = json.loads(tier_exits) if isinstance(tier_exits, str) else (tier_exits or [])
        except Exception:
            tier_exits = []
        sold_percent = sum(float(t.get('percent_sold', 0) or 0) for t in tier_exits)
        remaining_percent = max(0, min(100, int(round(100 - sold_percent))))
        highest_tier = 0
        for tier in tier_exits:
            try:
                highest_tier = max(highest_tier, int(tier.get('tier', 0) or 0))
            except Exception:
                continue
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

        # Grad exit profile override (director: fixed stop + time exit for grad trades)
        if exit_profile:
            stop_price = entry_price * (1.0 - exit_profile.get('stop_pct', 30) / 100.0)

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
            'highest_tier_reached': highest_tier,
            'tiers_executed': list(tier_exits),
            'remaining_percent': remaining_percent,
            'realized_usd': 0.0,
            'registered_at': time.time(),
            'last_update': time.time(),
            'wallet_trade_id': wallet_trade_id,
            'token_amount_raw': token_amount_raw,
            'token_decimals': token_decimals,
            'entry_sol_lamports': entry_sol_lamports,
            'wallet_sell_seq': 0,
            # Grad exit profile fields
            'grad_exit_profile': exit_profile,
            'grad_stop_price': stop_price if exit_profile else None,
            'grad_hold_deadline': (time.time() + exit_profile.get('hold_hours', 24) * 3600) if exit_profile else None,
            'grad_tiers': exit_profile.get('tiers', []) if exit_profile else [],
            'grad_trail_after_mult': exit_profile.get('trail_after_mult', 2) if exit_profile else None,
            'grad_trail_pct': exit_profile.get('trail_pct', 20) if exit_profile else None,
            'highest_grad_tier_reached': highest_tier,
        }
        logger.info(f"Registered pos {position_id}: {token_address[:8]}... entry ${entry_price:.8f} mcap ${entry_mcap:.0f} stop ${stop_price:.8f} ATR ${atr:.8f} buy ${buy_amount:.2f}")

    def _remaining_raw(self, state: dict) -> Optional[int]:
        total_raw = state.get('token_amount_raw')
        if not total_raw or int(total_raw) <= 0:
            return None
        remaining = max(0, min(100, int(round(float(state.get('remaining_percent', 0))))))
        raw = int(total_raw) * remaining // 100
        if self.demo_wallet:
            raw = min(raw, self.demo_wallet.token_balance_raw(state['token_address']))
        return raw if raw > 0 else None

    async def _paper_quote_mark(self, state: dict) -> Optional[dict]:
        raw = self._remaining_raw(state)
        if raw is None:
            return None
        quote = await self.demo_wallet.quote_sell_value(
            state['token_address'],
            raw,
            token_decimals=state.get('token_decimals'),
        )
        if not quote:
            return None
        remaining = max(0, min(100, int(round(float(state.get('remaining_percent', 0))))))
        entry_total = int(state.get('entry_sol_lamports') or 0)
        entry_remaining = entry_total * remaining // 100 if entry_total > 0 else 0
        pnl_percent = None
        if entry_remaining > 0:
            pnl_percent = float(
                (Decimal(quote['out_lamports']) - Decimal(entry_remaining))
                / Decimal(entry_remaining) * Decimal(100)
            )
        current_price = None
        human = quote.get('token_amount')
        if human and human > 0:
            sol_usd = self.demo_wallet.sol_price()
            current_price = float(
                (Decimal(quote['out_lamports']) / Decimal(1_000_000_000)) * sol_usd / human
            )
        return {
            'raw': raw,
            'out_lamports': quote['out_lamports'],
            'entry_sol_lamports_remaining': entry_remaining,
            'pnl_percent': pnl_percent,
            'current_price': current_price,
        }

    async def _check_all_positions(self):
        positions = self.db.get_open_positions()
        for position in positions:
            pos_id = position['id']
            if pos_id not in self.position_states:
                await self.register_position(
                    pos_id, position['entry_price'], position['token_address'],
                    position['chain'], position['token_amount'],
                    buy_amount=position.get('entry_amount_usd', 0),
                    wallet_trade_id=position.get('wallet_trade_id'),
                    token_amount_raw=position.get('token_amount_raw'),
                    token_decimals=position.get('token_decimals'),
                    entry_sol_lamports=position.get('entry_sol_lamports'),
                    tier_exits=position.get('tier_exits'),
                )
            state = self.position_states[pos_id]
            pnl_percent = None
            current_price = None
            if self.paper_mode and self.demo_wallet:
                mark = await self._paper_quote_mark(state)
                if mark:
                    pnl_percent = mark['pnl_percent']
                    current_price = mark['current_price']
                    state['pnl_percent'] = pnl_percent
                    state['pnl_source'] = 'jupiter_sell_quote'
                    state['exit_quote_lamports'] = mark['out_lamports']
                    state['entry_sol_lamports_remaining'] = mark['entry_sol_lamports_remaining']
                else:
                    state['pnl_percent'] = None
                    state['pnl_source'] = 'jupiter_quote_unavailable'
                    logger.warning(
                        "PnL unavailable for %s: no exact Jupiter sell quote",
                        position['token_address'][:8],
                    )
            else:
                current_price = await self.executor.get_token_price(position['token_address'], position['chain'], self.chains_config)
                if current_price <= 0:
                    continue
                pnl_percent = ((current_price - state['entry_price']) / state['entry_price']) * 100 if state['entry_price'] > 0 else 0

            if current_price is not None and current_price > 0:
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
                                atr_stop = lambda ep, s, a, m: ep * 0.70
                        new_stop = current_price - state['atr'] * self.atr_trailing_mult
                        if new_stop > state['stop_price']:
                            state['stop_price'] = new_stop
            exit_result = await self._check_exit_conditions(pos_id, state, pnl_percent, position)
            if exit_result:
                await self._execute_exit(pos_id, state, exit_result, position)

    async def _check_exit_conditions(self, pos_id: int, state: dict, pnl_percent: Optional[float], position: dict) -> Optional[dict]:
        # GRAD EXIT PROFILE (director: fixed stop + time exit + tiers + trail-after for grad trades)
        if state.get('grad_exit_profile'):
            ep = state['grad_exit_profile']
            # 1. Fixed stop (highest priority for grad)
            if state.get('grad_stop_price') and state['current_price'] <= state['grad_stop_price']:
                return {'reason': 'grad_stop', 'sell_percent': state['remaining_percent'],
                        'details': f"Grad stop ${state['grad_stop_price']:.8f} hit (entry ${state['entry_price']:.8f})"}
            # 2. Time exit (grad hold hours)
            if state.get('grad_hold_deadline') and time.time() >= state['grad_hold_deadline']:
                return {'reason': 'grad_time_exit', 'sell_percent': state['remaining_percent'],
                        'details': f'Grad hold {ep.get("hold_hours", 24)}h elapsed'}
            if pnl_percent is None:
                return None
            # 3. Tiered exits (grad multipliers)
            for i, tier in enumerate(state.get('grad_tiers', [])):
                tier_num = i + 1
                if tier_num <= state.get('highest_grad_tier_reached', 0):
                    continue
                if pnl_percent >= ((tier['multiplier'] - 1) * 100):
                    sell_percent = min(tier['sell_percent'], state['remaining_percent'])
                    return {'reason': f'grad_tier_{tier_num}', 'tier_multiplier': tier['multiplier'],
                            'sell_percent': sell_percent,
                            'details': f'Grad tier {tier_num} {tier["multiplier"]}x at {pnl_percent:.1f}%'}
            # 4. Trail only after trail_after_mult
            trail_after = state.get('grad_trail_after_mult', 2)
            if pnl_percent >= ((trail_after - 1) * 100):
                drawdown = ((state['peak_price'] - state['current_price']) / state['peak_price']) * 100 if state['peak_price'] > 0 else 0
                atr_breach = False
                if state['atr'] and state['atr'] > 0:
                    atr_breach = state['current_price'] < (state['peak_price'] - state['atr'] * self.atr_trailing_mult)
                trail_pct = state.get('grad_trail_pct', 20)
                if drawdown >= trail_pct or atr_breach:
                    return {'reason': 'grad_trailing', 'sell_percent': state['remaining_percent'],
                            'details': f'Grad trail: peak ${state["peak_price"]:.8f} -> ${state["current_price"]:.8f} dd {drawdown:.1f}%'}
            return None  # grad profile handled all exits; don't fall through to defaults

        # DEFAULT EXIT LOGIC (non-grad positions)
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
        if pnl_percent is not None and pnl_percent <= -self.emergency_loss:
            return {'reason': 'emergency_loss', 'sell_percent': state['remaining_percent'],
                    'details': f'Emergency loss {pnl_percent:.1f}%'}

        # 4. ATR trailing stop (after min profit) - more sensitive than fixed 20%
        if pnl_percent is not None and self.trailing_enabled and pnl_percent >= self.min_profit_trailing:
            # Two triggers: fixed % drawdown OR ATR breach from peak
            drawdown = ((state['peak_price'] - state['current_price']) / state['peak_price']) * 100 if state['peak_price'] > 0 else 0
            atr_breach = False
            if state['atr'] and state['atr'] > 0:
                atr_breach = state['current_price'] < (state['peak_price'] - state['atr'] * self.atr_trailing_mult)
            if drawdown >= self.trailing_callback or atr_breach:
                return {'reason': 'trailing_stop', 'sell_percent': state['remaining_percent'],
                        'details': f'Trailing: peak ${state["peak_price"]:.8f} -> ${state["current_price"]:.8f} drawdown {drawdown:.1f}% ATR {state["atr"]:.8f}'}

        # 5. Tiered exits
        if pnl_percent is not None:
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
        logger.info(f"Exit {pos_id}: {reason} sell {sell_percent}% - {exit_result.get('details', '')}")
        try:
            if self.paper_mode:
                if not self.demo_wallet:
                    logger.error(f"No demo wallet for paper exit {pos_id}")
                    return
                wallet_trade_id = state.get('wallet_trade_id') or str(pos_id)
                seq = int(state.get('wallet_sell_seq', 0)) + 1
                remaining = int(state.get('remaining_percent', 100))
                wallet_percent = 100 if int(sell_percent) >= remaining else int(sell_percent)
                fill = await self.demo_wallet.sell_percent(
                    state['token_address'],
                    wallet_percent,
                    trade_id=wallet_trade_id,
                    reason=reason,
                    idempotency_key=f"{wallet_trade_id}:sell:{seq}:{reason}",
                    token_decimals=state.get('token_decimals'),
                    basis_raw=state.get('token_amount_raw'),
                )
                if not fill:
                    logger.error(f"Paper exit refused by ledger wallet {pos_id} ({reason})")
                    return
                state['wallet_sell_seq'] = seq
                net_usd = float(fill['net_usd'])
                state['realized_usd'] = state.get('realized_usd', 0.0) + net_usd
                result = {'success': True, 'paper': True, 'tx_hash': f"paper_{pos_id}_{reason}", 'net_usd': net_usd, 'fill': fill}
                logger.info(f"[PAPER] Sell {sell_percent}% pos {pos_id} {reason} - Jupiter exact-in + gas/rent: ${net_usd:.4f} net")
            else:
                result = await self.executor.sell_token(state['token_address'], state['chain'], state['token_amount'], sell_percent, self.chains_config)
            if result and result.get('success'):
                state['remaining_percent'] -= sell_percent
                if reason.startswith('tier_') or reason.startswith('grad_tier_'):
                    tier_num = int(reason.split('_')[-1])
                    if reason.startswith('grad_tier_'):
                        state['highest_grad_tier_reached'] = tier_num
                    else:
                        state['highest_tier_reached'] = tier_num
                    state['tiers_executed'].append({'tier': tier_num, 'multiplier': exit_result.get('tier_multiplier'), 'price': state['current_price'], 'percent_sold': sell_percent, 'net_usd': result.get('net_usd', 0), 'timestamp': datetime.now().isoformat()})
                    self.db.update_position_tier(pos_id, state['tiers_executed'])
                logger.info(f"Sold {sell_percent}% pos {pos_id} {state['token_address'][:8]}... ({reason}) remaining {state['remaining_percent']}%")
                # TG push ONLY for positive exits (tier profits, profitable trails, grad tiers)
                positive_reasons = ('tier_1', 'tier_2', 'tier_3', 'grad_tier_1', 'grad_tier_2', 'grad_tier_3',
                                    'trailing_stop', 'grad_trailing', 'grad_time_exit')
                is_positive = reason in positive_reasons or (reason == 'trailing_stop' and state.get('realized_usd', 0) > 0)
                if is_positive and self.tg_bot:
                    pnl_pct = state.get('pnl_percent')
                    if pnl_pct is None:
                        pnl_pct = ((state['current_price'] - state['entry_price']) / state['entry_price']) * 100 if state['entry_price'] > 0 else 0
                    self.tg_bot.broadcast(
                        f"🟢 <b>WIN {reason}</b> {state['token_address'][:6]}...{state['token_address'][-4:]}\n"
                        f"Sold {sell_percent}% @ ${state['current_price']:.8f} ({pnl_pct:+.1f}%)\n"
                        f"Remaining {state['remaining_percent']}% for moonshot 🚀\n"
                        f"<code>{state['token_address']}</code>"
                    )
                if state['remaining_percent'] <= 0:
                    await self._close_position(pos_id, state, position, reason)
            else:
                logger.error(f"Exit failed {pos_id}: {result}")
        except Exception as e:
            logger.error(f"Exit exec error {pos_id}: {e}")

    async def _close_position(self, pos_id: int, state: dict, position: dict, reason: str):
        try:
            if self.paper_mode:
                remaining_percent = int(round(float(state.get('remaining_percent', 0))))
                if remaining_percent > 0:
                    if not self.demo_wallet:
                        logger.error(f"No demo wallet to close paper position {pos_id}")
                        return
                    wallet_trade_id = state.get('wallet_trade_id') or str(pos_id)
                    seq = int(state.get('wallet_sell_seq', 0)) + 1
                    fill = await self.demo_wallet.sell_percent(
                        state['token_address'],
                        100,
                        trade_id=wallet_trade_id,
                        reason=reason + '_final',
                        idempotency_key=f"{wallet_trade_id}:sell:{seq}:{reason}_final",
                        token_decimals=state.get('token_decimals'),
                        basis_raw=state.get('token_amount_raw'),
                    )
                    if not fill:
                        logger.error(f"Final paper sell refused for {pos_id}; leaving position open")
                        return
                    state['wallet_sell_seq'] = seq
                    state['realized_usd'] = state.get('realized_usd', 0.0) + float(fill['net_usd'])
                exit_price = state['current_price']
            else:
                exit_price = state['current_price']
                if state['remaining_percent'] > 0:
                    result = await self.executor.sell_token(state['token_address'], state['chain'], state['token_amount'], state['remaining_percent'], self.chains_config)
                    if not result or not result.get('success'):
                        logger.error(f"Final live sell failed for {pos_id}; leaving position open")
                        return
                    state['realized_usd'] = state.get('realized_usd', 0.0) + float(result.get('net_usd', 0) or 0)

            self.db.close_position(
                pos_id, exit_price, float(state.get('realized_usd', 0.0)), reason,
                state.get('tiers_executed', [])
            )
            if self.memory:
                try:
                    hold_minutes = (time.time() - state['registered_at']) / 60
                    feats = self.memory._featurize(
                        state['token_address'], state['chain'], state['entry_price'],
                        liquidity=0, mcap=state.get('entry_mcap', 0), scout_score=50
                    )
                    self.memory.record_close(
                        state['token_address'], state['chain'], state['entry_price'],
                        exit_price, hold_minutes, feats,
                        entry_mcap=state.get('entry_mcap', 0), exit_mcap=0,
                        buy_amount=state.get('buy_amount', 0),
                        token_amount=state.get('token_amount', 0),
                    )
                except Exception as e:
                    logger.debug(f"Memory record error: {e}")
            logger.info(f"Closed pos {pos_id} {state['token_address'][:8]}... {reason} realized ${state['realized_usd']:.2f}")
            # Callback for MiniTransformer online update (director: learn from every close)
            if self.on_position_close:
                try:
                    label = 1 if state.get('realized_usd', 0) > 0 else 0
                    # Pass full state for feature building in callback
                    close_data = {
                        'token_address': state['token_address'],
                        'chain': state['chain'],
                        'entry_price': state['entry_price'],
                        'exit_price': state['current_price'],
                        'realized_usd': state.get('realized_usd', 0),
                        'entry_mcap': state.get('entry_mcap', 0),
                        'buy_amount': state.get('buy_amount', 0),
                        'reason': reason,
                        'grad_exit': state.get('grad_exit_profile') is not None,
                    }
                    self.on_position_close(label, close_data)
                except Exception as e:
                    logger.debug(f"Position close callback error: {e}")
            del self.position_states[pos_id]
        except Exception as e:
            logger.error(f"Close position error {pos_id}: {e}")

    def get_position_state(self, position_id: int) -> Optional[dict]:
        return self.position_states.get(position_id)

    def get_all_states(self) -> Dict[int, dict]:
        return self.position_states.copy()
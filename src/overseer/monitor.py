"""Overseer continuous monitor - after a coin is bought and keeps rising, overseer watches it constantly.
If anything negative/positive happens, sends info to alerts (TG) and execution layer simultaneously.
"""
import asyncio
import logging
import time
from typing import Dict

logger = logging.getLogger(__name__)

class OverseerMonitor:
    def __init__(self, config: dict, overseer, alert_manager, exit_manager, executor, db):
        self.config = config
        self.overseer = overseer
        self.alert_manager = alert_manager
        self.exit_manager = exit_manager
        self.executor = executor
        self.db = db
        self.running = False
        self.last_checks: Dict[int, Dict] = {}
        self.tg_bot = None

    async def start(self, chains_config: dict):
        self.running = True
        self.chains_config = chains_config
        logger.info("OverseerMonitor started - continuously monitoring open positions for +/- signals")
        while self.running:
            try:
                await self._check_all()
            except Exception as e:
                logger.debug(f"OverseerMonitor error: {e}")
            await asyncio.sleep(15)

    async def stop(self):
        self.running = False

    async def _check_all(self):
        positions = self.db.get_open_positions()
        for pos in positions:
            pid = pos['id']
            token = pos['token_address']
            chain = pos['chain']
            entry = float(pos['entry_price'] or 0)
            # Get live data
            live = await self.executor.get_token_live_data(token, chain, self.chains_config)
            cur_price = live.get('priceUsd', 0) or entry
            mcap = live.get('mcap', 0)
            liq = live.get('liquidityUsd', 0)
            vol = live.get('volume24h', 0)

            # Track last check
            last = self.last_checks.get(pid)
            if not last:
                self.last_checks[pid] = {'price': cur_price, 'mcap': mcap, 'liq': liq, 'time': time.time()}
                continue

            # Positive: price rising >15% since last check and volume up
            price_chg = ((cur_price - last['price'])/last['price']*100) if last['price'] else 0
            liq_chg = ((liq - last['liq'])/last['liq']*100) if last['liq'] else 0

            alert_needed = None
            log_alert = None
            if liq_chg < -20:
                alert_needed = f"🚨 NEGATIVE dev dump? liquidity {liq_chg:+.1f}% ${last['liq']:.0f} -> ${liq:.0f}"
                log_alert = f"NEGATIVE dev dump liquidity {liq_chg:+.1f}%"
            elif price_chg < -12:
                alert_needed = f"⚠️ NEGATIVE price -{abs(price_chg):.1f}% ${last['price']:.6f} -> ${cur_price:.6f}"
                log_alert = f"NEGATIVE price -{abs(price_chg):.1f}%"
            elif price_chg > 18 and vol > 0:
                alert_needed = f"✅ POSITIVE rising +{price_chg:.1f}% mcap ${mcap:.0f} vol ${vol:.0f}"
                log_alert = f"POSITIVE rising +{price_chg:.1f}%"

            if alert_needed:
                # Send to alerts (TG + execution)
                try:
                    # Alerts layer
                    if self.alert_manager:
                        # Create synthetic alert for this coin's movement
                        await self.alert_manager.add_watch({'wallet': token[:10], 'chain': chain, 'grade': 'MONITOR', 'score': 70, 'coin_symbol': pos.get('token_symbol','???'), 'coin': token})
                    # Execution layer: tighten stop if negative, or hold if positive
                    if "NEGATIVE" in alert_needed and self.exit_manager:
                        state = self.exit_manager.get_position_state(pid)
                        if state:
                            state['stop_price'] = max(state.get('stop_price',0), cur_price * 0.85)
                            logger.info(f"OverseerMonitor tightened stop for {token[:8]}... to ${state['stop_price']:.6f} due to {log_alert}")
                    tg = getattr(self, 'tg_bot', None) or getattr(self.alert_manager, 'tg_bot', None) or getattr(self.exit_manager, 'tg_bot', None) if hasattr(self, 'alert_manager') and hasattr(self, 'exit_manager') else getattr(self, 'tg_bot', None)
                    if tg and hasattr(tg, 'broadcast'):
                        tg.broadcast(f"{alert_needed}\n{pos.get('token_symbol','???')} <code>{token}</code> {chain} entry ${entry:.6f} -> ${cur_price:.6f} mcap ${mcap:.0f} liq ${liq:.0f}\n<a href='https://dexscreener.com/{chain}/{token}'>dex</a>")
                    logger.info(f"OverseerMonitor {token[:8]}... {log_alert}")
                except Exception as e:
                    logger.debug(f"OverseerMonitor alert error: {e}")

            # Update last
            self.last_checks[pid] = {'price': cur_price, 'mcap': mcap, 'liq': liq, 'time': time.time()}

        # Clean up closed positions
        open_ids = {p['id'] for p in positions}
        for pid in list(self.last_checks.keys()):
            if pid not in open_ids:
                del self.last_checks[pid]

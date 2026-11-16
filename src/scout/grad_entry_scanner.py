"""Sol graduation-entry scanner - the director's edge, live.

PumpPortal free WS (subscribeMigration) -> on each migration:
1. SECURITY VET (hard requirement): mint/freeze authority must be revoked.
   Unvettable token = SKIP (same policy as overseer hard-skip, non-negotiable).
2. PRICE: DexScreener retries up to ~4min (new PumpSwap pool indexing lag).
3. Emit coin signal source='pumpfun_migration' into the engine pipeline.
   The pipeline routes grad signals PAST the overseer blend (scanner already
   vetted security) but keeps all sizing/risk/halt/exit machinery.

Backtest basis (2026-11-14): entry at graduation, stop30/24h: June +112% July +60%
EV, month-robust sample n=250; GT-open == first-trade-price validated live (2 MATCH).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time

logger = logging.getLogger(__name__)

WS_URL = "wss://pumpportal.fun/api/data"


class GradEntryScanner:
    def __init__(self, config, signal_callback):
        self.config = config
        self.signal_callback = signal_callback
        g = config.get('grad_entry', {}) or {}
        self.enabled = g.get('enabled', False)
        self.price_timeout_s = g.get('price_timeout_seconds', 240)
        self.price_poll_s = g.get('price_poll_seconds', 20)
        self.score = g.get('score', 75)
        self.max_mcap_usd = g.get('max_mcap_usd', 0)
        self.min_liquidity_usd = g.get('min_liquidity_usd', 0)
        # risk multiplier ladder for grad trades (director: base 5%, high 8%, very_high 10%)
        rm = g.get('risk_mult', {}) or {}
        self.risk_base_pct = rm.get('base_pct', 5.0)
        self.risk_high_score = rm.get('high_score', 80)
        self.risk_high_pct = rm.get('high_pct', 8.0)
        self.risk_vhigh_score = rm.get('very_high_score', 90)
        self.risk_vhigh_pct = rm.get('very_high_pct', 10.0)
        # exit profile (optional, gated)
        ep = g.get('exit_profile', {}) or {}
        self.exit_profile = ep if ep.get('enabled') else None
        self._running = False
        self.migrations_seen = 0
        self.trades_emitted = 0
        self.vetoed = 0
        self._recent = {}  # mint -> last emit ts (WS can double-deliver a migration; dedupe)

    async def _security_vet(self, mint: str) -> bool:
        """Hard safety: solana token MUST have mint+freeze authority revoked to trade."""
        try:
            try:
                from agents.solana_agents import SolanaSecurityAgent
            except ImportError:
                from src.agents.solana_agents import SolanaSecurityAgent
            agent = SolanaSecurityAgent(self.config, self.config.get('chains', {}))
            res = await agent.check(mint, 'solana', {'token_address': mint})
            if not res.get('available'):
                logger.warning(f"GRAD {mint[:8]}... security agent unavailable -> HARD SKIP (never trade unvetted)")
                return False
            flag = res.get('flag', '')
            if flag in ('MINT_AUTH_ACTIVE', 'FREEZE_AUTH_ACTIVE'):
                logger.warning(f"GRAD {mint[:8]}... VETO {flag} -> SKIP")
                return False
            return True
        except Exception as e:
            logger.error(f"GRAD {mint[:8]}... security vet error {e} -> HARD SKIP")
            return False

    async def _wait_price(self, mint: str):
        """DexScreener retry loop - fresh PumpSwap pools index with a lag."""
        try:
            import aiohttp
        except ImportError:
            logger.error("aiohttp missing - cannot fetch grad price")
            return None
        deadline = time.time() + self.price_timeout_s
        url = f"https://api.dexscreener.com/tokens/v1/solana/{mint}"
        async with aiohttp.ClientSession() as s:
            while time.time() < deadline:
                try:
                    async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status == 200:
                            data = await r.json()
                            if isinstance(data, list) and data:
                                p = data[0]
                                price = float(p.get('priceUsd', 0) or 0)
                                if price > 0:
                                    return {
                                        'priceUsd': price,
                                        'mcap': float(p.get('marketCap', 0) or p.get('fdv', 0) or 0),
                                        'liquidityUsd': float(p.get('liquidity', {}).get('usd', 0) or 0) if isinstance(p.get('liquidity'), dict) else 0.0,
                                        'symbol': p.get('baseToken', {}).get('symbol', '') if isinstance(p.get('baseToken'), dict) else '',
                                    }
                except Exception as e:
                    logger.debug(f"GRAD price poll error {mint[:8]}: {e}")
                await asyncio.sleep(self.price_poll_s)
        return None

    async def _handle_migration(self, msg: dict):
        mint = msg.get('mint', '')
        if not mint:
            return
        self.migrations_seen += 1
        now = time.time()
        if now - self._recent.get(mint, 0) < 300:
            logger.info(f"GRAD {mint[:8]}... duplicate migration event within 5min -> ignored")
            return
        self._recent[mint] = now
        if len(self._recent) > 500:
            self._recent = {k: v for k, v in self._recent.items() if now - v < 3600}
        logger.info(f"GRAD MIGRATION {mint[:12]}... ({self.migrations_seen} seen)")
        if not await self._security_vet(mint):
            self.vetoed += 1
            return
        px = await self._wait_price(mint)
        if not px or px['priceUsd'] <= 0:
            logger.warning(f"GRAD {mint[:8]}... no live price within {self.price_timeout_s}s -> SKIP (never buy blind)")
            return
        if self.max_mcap_usd and px['mcap'] > self.max_mcap_usd:
            logger.info(f"GRAD {mint[:8]}... mcap ${px['mcap']:.0f} above cap -> SKIP")
            return
        if self.min_liquidity_usd and px['liquidityUsd'] < self.min_liquidity_usd:
            logger.info(f"GRAD {mint[:8]}... liq ${px['liquidityUsd']:.0f} below min -> SKIP")
            return
        # risk multiplier for grad trades (overrides global confidence_scaling)
        if self.score >= self.risk_vhigh_score:
            risk_cap_pct = self.risk_vhigh_pct
        elif self.score >= self.risk_high_score:
            risk_cap_pct = self.risk_high_pct
        else:
            risk_cap_pct = self.risk_base_pct
        signal = {
            'token_address': mint,
            'chain': 'solana',
            'source': 'pumpfun_migration',
            'price': px['priceUsd'],
            'mcap': px['mcap'],
            'liquidity_usd': px['liquidityUsd'],
            'score': self.score,
            'threshold_used': 0,  # score gate bypassed: edge is the graduation event itself
            'token_info': {'symbol': px['symbol'], 'name': px['symbol']},
            'graduated_at': int(time.time()),
            'grad_risk_cap_pct': risk_cap_pct,
            'grad_exit_profile': self.exit_profile,
        }
        self.trades_emitted += 1
        logger.info(f"GRAD SIGNAL {mint[:8]}... ${px['priceUsd']:.8f} mcap ${px['mcap']:.0f} -> pipeline")
        await self.signal_callback(signal)

    async def start(self, chains: dict):
        if not self.enabled:
            logger.info("GradEntryScanner disabled (grad_entry.enabled=false)")
            return
        self._running = True
        logger.info("GradEntryScanner: PumpPortal migration stream starting...")
        attempt = 0
        while self._running:
            try:
                import websockets
                async with websockets.connect(WS_URL, ping_interval=20) as ws:
                    attempt = 0
                    await ws.send(json.dumps({"method": "subscribeMigration"}))
                    logger.info("GradEntryScanner: subscribed to migration stream")
                    async for raw in ws:
                        if not self._running:
                            return
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if msg.get('txType') in ('migrate', 'migration'):
                            asyncio.create_task(self._handle_migration(msg))
            except Exception as e:
                attempt += 1
                delay = min(2 ** attempt, 60)
                logger.warning(f"GradEntryScanner WS error {repr(e)[:80]}; retry in {delay}s")
                await asyncio.sleep(delay)

    async def stop(self):
        self._running = False
        logger.info("GradEntryScanner stopped")

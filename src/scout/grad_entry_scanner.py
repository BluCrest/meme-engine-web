"""Sol graduation-entry scanner - the director's edge, live.

PumpPortal free WS (subscribeMigration) -> on each migration:
1. SECURITY VET (hard requirement): mint/freeze authority must be revoked.
   Unvettable token = SKIP (same policy as overseer hard-skip, non-negotiable).
2. ENTRY PRICE: executable Jupiter quote (SOL->token probe) with retries while the
   fresh PumpSwap pool becomes routable. This matches the tested edge (entry at
   migration / first-trade price) instead of waiting minutes for DexScreener,
   whose fresh-pool mark lags and can sit far off the tradeable price.
   No quote = SKIP (never buy blind).
3. Emit coin signal source='pumpfun_migration' into the engine pipeline.
   The pipeline routes grad signals PAST the overseer blend/score gate (scanner
   already vetted security) but keeps all sizing/risk/halt/exit machinery.

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
SOL_MINT = "So11111111111111111111111111111111111111112"


class GradEntryScanner:
    def __init__(self, config, signal_callback, wallet=None):
        self.config = config
        self.signal_callback = signal_callback
        self.wallet = wallet
        g = config.get('grad_entry', {}) or {}
        self.enabled = g.get('enabled', False)
        # Executable-price window: fresh pools need a few seconds to become routable.
        self.quote_timeout_s = int(g.get('quote_timeout_seconds', 90))
        self.quote_poll_s = float(g.get('quote_poll_seconds', 3))
        self.quote_probe_sol = float(g.get('quote_probe_sol', 0.01))
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
        self.unpriced = 0
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

    async def _wait_quote(self, mint: str):
        """First executable SOL->token quote for the fresh pool (entry truth)."""
        if not self.wallet:
            logger.error(f"GRAD {mint[:8]}... no wallet attached - cannot price -> SKIP")
            return None
        probe = int(self.quote_probe_sol * 1_000_000_000)
        deadline = time.time() + self.quote_timeout_s
        last_err = ''
        while time.time() < deadline:
            try:
                decimals = await self.wallet._resolve_decimals(mint)
                if decimals is not None:
                    quote = await self.wallet.quote_client.quote(
                        SOL_MINT, mint, probe, self.wallet.slippage_bps
                    )
                    tokens = int(quote['out_amount']) / (10 ** int(decimals))
                    sol_usd = float(self.wallet.sol_price())
                    if tokens > 0 and sol_usd > 0:
                        price = (probe / 1_000_000_000) * sol_usd / tokens
                        if price > 0:
                            return price, quote
            except Exception as exc:
                last_err = str(exc)[:120]
            await asyncio.sleep(self.quote_poll_s)
        logger.warning(
            f"GRAD {mint[:8]}... no executable Jupiter quote in {self.quote_timeout_s}s"
            f"{(' ' + last_err) if last_err else ''} -> SKIP (never buy blind)"
        )
        return None

    async def _dex_meta(self, mint: str) -> dict:
        """Best-effort metadata (mcap/liq/symbol) - never blocks the entry."""
        try:
            import aiohttp
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"https://api.dexscreener.com/tokens/v1/solana/{mint}",
                    timeout=aiohttp.ClientTimeout(total=4),
                ) as r:
                    if r.status != 200:
                        return {}
                    data = await r.json()
            if not (isinstance(data, list) and data):
                return {}
            p = data[0]
            liq = p.get('liquidity')
            base = p.get('baseToken') if isinstance(p.get('baseToken'), dict) else {}
            return {
                'mcap': float(p.get('marketCap') or p.get('fdv') or 0),
                'liquidityUsd': float(liq.get('usd', 0) or 0) if isinstance(liq, dict) else 0.0,
                'symbol': base.get('symbol', '') or '',
            }
        except Exception:
            return {}

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
        priced = await self._wait_quote(mint)
        if not priced:
            self.unpriced += 1
            return
        price, _quote = priced
        meta = await self._dex_meta(mint)
        mcap = meta.get('mcap', 0) or 0
        liq = meta.get('liquidityUsd', 0) or 0
        if self.max_mcap_usd and mcap and mcap > self.max_mcap_usd:
            logger.info(f"GRAD {mint[:8]}... mcap ${mcap:.0f} above cap ${self.max_mcap_usd:.0f} -> SKIP")
            return
        if self.min_liquidity_usd and liq and liq < self.min_liquidity_usd:
            logger.info(f"GRAD {mint[:8]}... liq ${liq:.0f} below min ${self.min_liquidity_usd:.0f} -> SKIP")
            return
        # risk multiplier for grad trades (overrides global confidence_scaling)
        if self.score >= self.risk_vhigh_score:
            risk_cap_pct = self.risk_vhigh_pct
        elif self.score >= self.risk_high_score:
            risk_cap_pct = self.risk_high_pct
        else:
            risk_cap_pct = self.risk_base_pct
        symbol = meta.get('symbol') or '???'
        signal = {
            'token_address': mint,
            'chain': 'solana',
            'source': 'pumpfun_migration',
            'price': price,
            'mcap': mcap,
            'liquidity_usd': liq,
            'score': self.score,
            'threshold_used': 0,  # score gate bypassed: edge is the graduation event itself
            'token_info': {'symbol': symbol, 'name': symbol},
            'graduated_at': int(time.time()),
            'grad_risk_cap_pct': risk_cap_pct,
            'grad_exit_profile': self.exit_profile,
        }
        self.trades_emitted += 1
        logger.info(
            f"GRAD DIRECT {mint[:8]}... ${price:.8f} mcap ${mcap:.0f} liq ${liq:.0f}"
            f" (executable Jupiter quote) -> pipeline"
        )
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

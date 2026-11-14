"""Overseer - coin-triggered wallet investigation fan-out.
Flow: scout finds COIN (new pair / volume spike) -> overseer deploys agents IN PARALLEL to vet WALLETS touching that coin.
Reuses brutal-wallet-auditor grading (C:/Users/EMMANUEL/brutal-wallet-auditor/src/grading_system.py) via Ankr.
"""
from __future__ import annotations
import asyncio
import logging
import time
from typing import Dict, List, Callable
from datetime import datetime

logger = logging.getLogger(__name__)

class Overseer:
    def __init__(self, config: dict):
        self.config = config
        self.ankr_key = config.get('ankr', {}).get('api_key', '')
        # Thresholds - lenient bootstrap for first trades so we don't fade runners (memes: brutal checks too strict early)
        overseer_cfg = config.get('overseer', {})
        self.min_overall_score = overseer_cfg.get('min_overall_score', 60)
        self.lenient_min_score = overseer_cfg.get('lenient_min_score', 45)
        self.lenient_until_trades = overseer_cfg.get('lenient_until_trades', 20)
        self.agent_timeout = overseer_cfg.get('agent_timeout_seconds', 12)
        # Lenient weights: reduce dev penalty early (memes: unknown dev is normal)
        self.strict_weights = overseer_cfg.get('weights', {'dev': 0.35, 'holders': 0.25, 'kols': 0.15, 'profit_wallets': 0.25})
        self.lenient_weights = overseer_cfg.get('lenient_weights', {'dev': 0.20, 'holders': 0.30, 'kols': 0.10, 'profit_wallets': 0.40})
        # Solana hard safety: if the security agent (mint/freeze authority) cannot vet
        # the token, do NOT trade on blended guesses - SKIP. Config: overseer.require_security_solana
        self.require_security_solana = overseer_cfg.get('require_security_solana', True)

    async def investigate(self, coin_signal: Dict, chains_config: dict) -> Dict:
        """Fan-out to wallet agents for a coin. Returns enriched signal with wallet scores.
        Solana: real agents (security/holders/dev) vetting the actual token.
        Unavailable agents are EXCLUDED from the blend and weights renormalized - no corpse votes."""
        token = coin_signal.get('token_address', '')
        chain = coin_signal.get('chain', 'solana')
        start = time.time()

        if chain == 'solana':
            try:
                from agents.solana_agents import SolanaSecurityAgent, SolanaHoldersAgent, SolanaDevAgent
            except ImportError:
                from src.agents.solana_agents import SolanaSecurityAgent, SolanaHoldersAgent, SolanaDevAgent
            agent_objs = {
                'security': SolanaSecurityAgent(self.config, chains_config),
                'holders': SolanaHoldersAgent(self.config, chains_config),
                'dev': SolanaDevAgent(self.config, chains_config),
            }
            weights_cfg = {'security': 0.20, 'holders': 0.50, 'dev': 0.30}
            lenient_weights_cfg = {'security': 0.20, 'holders': 0.50, 'dev': 0.30}
            penalize_flags = ('MINT_AUTH_ACTIVE', 'FREEZE_AUTH_ACTIVE', 'BUNDLED', 'SERIAL_CREATOR')
            tasks = {name: asyncio.create_task(obj.check(token, chain, coin_signal))
                     for name, obj in agent_objs.items()}
        else:
            # Import agents lazily - handle both src/* and flat path
            try:
                from agents.dev_agent import DevAgent
                from agents.holders_agent import HoldersAgent
                from agents.kol_agent import KolAgent
                from agents.profit_agent import ProfitAgent
            except ImportError:
                from src.agents.dev_agent import DevAgent
                from src.agents.holders_agent import HoldersAgent
                from src.agents.kol_agent import KolAgent
                from src.agents.profit_agent import ProfitAgent

            # Instantiate agents (each uses Ankr + DexScreener)
            dev_agent = DevAgent(self.config, chains_config)
            holders_agent = HoldersAgent(self.config, chains_config)
            kol_agent = KolAgent(self.config, chains_config)
            profit_agent = ProfitAgent(self.config, chains_config)

            tasks = {
                'dev': asyncio.create_task(dev_agent.check(token, chain, coin_signal)),
                'holders': asyncio.create_task(holders_agent.check(token, chain, coin_signal)),
                'kols': asyncio.create_task(kol_agent.check(token, chain, coin_signal)),
                'profit_wallets': asyncio.create_task(profit_agent.check(token, chain, coin_signal)),
            }
            weights_cfg = self.strict_weights
            lenient_weights_cfg = self.lenient_weights
            penalize_flags = ('RUG_HISTORY', 'HONEYPOT')  # strict set applied below

        results = {}
        for name, task in tasks.items():
            try:
                results[name] = await asyncio.wait_for(task, timeout=self.agent_timeout)
            except asyncio.TimeoutError:
                logger.warning(f"Overseer agent {name} timed out for {token[:8]}...")
                results[name] = {'available': False, 'score': 50, 'flag': 'TIMEOUT', 'details': 'agent timed out'}
            except Exception as e:
                logger.debug(f"Overseer agent {name} error: {e}")
                results[name] = {'available': False, 'score': 50, 'flag': 'ERROR', 'details': str(e)[:120]}

        is_lenient = False
        lenient_reason = ""
        try:
            total_trades = coin_signal.get('_total_trades', None)
            if total_trades is None:
                try:
                    from utils.database import TradeDatabase
                except ImportError:
                    from src.utils.database import TradeDatabase
                db = TradeDatabase()
                total_trades = db.get_stats().get('total_trades', 0)
                db.close()
            if total_trades < self.lenient_until_trades:
                is_lenient = True
                lenient_reason = f"bootstrap {total_trades}/{self.lenient_until_trades}"
        except:
            pass
        # Early-cap robinhood: even more lenient to learn faster (user wants more early caps)
        early_cap = False
        try:
            mcap = float(coin_signal.get('mcap',0) or coin_signal.get('liquidity_usd',0) or 0)
            # DexScreener mcap for robinhood early caps is often <100k
            if coin_signal.get('chain')=='robinhood' and mcap and mcap < 100000:
                early_cap = True
                is_lenient = True
                lenient_reason += " early-cap<100k"
        except: pass

        if is_lenient:
            weights = dict(lenient_weights_cfg)
            threshold = self.lenient_min_score
            if early_cap:
                threshold = min(threshold, 40)  # early-cap <100k needs more shots to learn
            if chain != 'solana':
                penalize_flags = ('RUG_HISTORY', 'HONEYPOT')
        else:
            weights = dict(weights_cfg)
            threshold = self.min_overall_score
            if early_cap:
                threshold = 50  # even strict mode, early-cap gets a bit lenient
            if chain != 'solana':
                penalize_flags = ('RUG_HISTORY', 'BUNDLED', 'HONEYPOT', 'DEV_DUMP')

        # Blend ONLY available agents - renormalize weights, no corpse votes at neutral 50
        live = {k: r for k, r in results.items() if r.get('available', True)}
        weight_sum = sum(weights.get(k, 0) for k in live)
        overall = 0
        flags = []
        if live and weight_sum > 0:
            for k, r in live.items():
                s = r.get('score', 50)
                if r.get('flag') in penalize_flags:
                    s = min(s, 20)
                if is_lenient and s < 30 and r.get('flag') not in penalize_flags:
                    s = 30
                overall += s * weights.get(k, 0)
                flags.append(f"{k}:{r.get('flag','?')}({s})")
            overall = overall / weight_sum
        else:
            flags.append('no live agents - scout score only')
        for k, r in results.items():
            if not r.get('available', True):
                flags.append(f"{k}:EXCLUDED({r.get('flag','?')})")

        overall = max(0, min(100, round(overall, 1)))
        # Boost original scout score - lenient gives scout more weight (0.5 vs 0.4) so good momentum not faded
        scout_score = coin_signal.get('score', 50)
        scout_weight = 0.5 if is_lenient else 0.4
        blended = round(overall * (1 - scout_weight) + scout_score * scout_weight, 1)

        # Hard veto: creator-held mint/freeze authority on solana = dev can mint infinite
        # supply / freeze your tokens at will. Weight blending dilutes this (0.20) so it
        # must be a veto, not a nudge. Program-held authorities are whitelisted in the
        # agent and never carry these flags.
        veto = False
        if chain == 'solana':
            for r in results.values():
                if r.get('available') and r.get('flag') in ('MINT_AUTH_ACTIVE', 'FREEZE_AUTH_ACTIVE'):
                    veto = True
                    break

        verdict = 'SKIP' if veto else ('BUY' if blended >= threshold else 'SKIP')
        # Hard safety: solana token not vetted by security agent (RPC fail/timeout) = untradeable
        if chain == 'solana' and self.require_security_solana and not results.get('security', {}).get('available'):
            verdict = 'SKIP'
            flags.append('SECURITY_UNVETTED_HARD_SKIP')
            logger.info(f"Overseer HARD-SKIP {token[:8]}... security agent unavailable (mint/freeze authority unknown) - refusing to trade unvetted token")

        enriched = {
            **coin_signal,
            'overseer_score': overall,
            'blended_score': blended,
            'overseer_flags': flags,
            'agent_results': results,
            'overseer_time_ms': int((time.time() - start)*1000),
            'is_lenient': is_lenient,
            'veto': veto,
            'verdict': verdict,
            'threshold_used': threshold,
            'reasons': self._reasons(results, blended, threshold)
        }

        mode = "LENIENT" if is_lenient else "STRICT"
        if veto:
            mode += " VETO"
        logger.info(f"Overseer {mode} {lenient_reason} {token[:8]}... on {chain}: scout {scout_score} -> wallets {overall} -> blended {blended}/{threshold} verdict {verdict} ({', '.join(flags)}) {enriched['overseer_time_ms']}ms")
        return enriched

    def _reasons(self, results: Dict, blended: float, threshold: float = None) -> List[str]:
        th = threshold if threshold is not None else self.min_overall_score
        reasons = []
        for name, r in results.items():
            if r.get('score', 50) >= 70:
                reasons.append(f"{name} strong: {r.get('flag')}")
            elif r.get('score', 50) <= 30:
                reasons.append(f"{name} weak: {r.get('flag')} - {r.get('details','')[:60]}")
        if blended >= th:
            reasons.insert(0, f"Blended {blended} >= {th} -> BUY")
        else:
            reasons.insert(0, f"Blended {blended} < {th} -> SKIP")
        return reasons[:5]

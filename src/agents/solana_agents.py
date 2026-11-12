"""Real Solana confidence agents - vet the actual token, not proxies.

SolanaSecurityAgent : mint/freeze authority revocation (hard rug vector)
SolanaHoldersAgent  : REAL top-20 holders via RPC, bundle/concentration detection,
                      creator-wallet holding %, owner mapping via getMultipleAccounts
SolanaDevAgent      : creator activity history via getSignaturesForAddress

All agents return {'available': bool, 'score': 0-100, 'flag': str, 'details': str}.
Unavailable agents are EXCLUDED from the Overseer blend (renormalized weights) -
no more corpse votes at neutral 50.
"""
import logging
import time
import aiohttp
from typing import Dict, Optional

logger = logging.getLogger(__name__)

PUMP_FUN_PROGRAM = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'


class SolanaRPC:
    """RPC client with URL fallback chain + 429 retry (public RPC rate-limits hard)."""

    def __init__(self, rpc_url: str, fallback_urls: list = None, helius_key: str = None):
        urls = [rpc_url or 'https://api.mainnet-beta.solana.com']
        for u in (fallback_urls or []):
            if u and u not in urls and not u.startswith('${'):
                urls.append(u)
        # Helius last - free tier, highest rate limit; activates when key present in config/.env
        if helius_key and not helius_key.startswith('${'):
            urls.append(f'https://mainnet.helius-rpc.com/?api-key={helius_key}')
        self.urls = urls

    async def call(self, method: str, params: list, timeout: int = 10) -> Optional[dict]:
        for url in self.urls:
            for attempt in range(2):
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.post(url, json={
                            "jsonrpc": "2.0", "id": 1, "method": method, "params": params
                        }, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                            if r.status == 429:
                                await asyncio.sleep(1.0 + attempt)
                                continue
                            if r.status != 200:
                                break
                            data = await r.json()
                            if data.get('error'):
                                break
                            return data.get('result')
                except Exception:
                    break
        return None


class SolanaSecurityAgent:
    """Mint/freeze authority check - unrevoked authority on a pump.fun token is a
    hard rug vector (dev can mint infinite supply / freeze your tokens)."""

    def __init__(self, config: dict, chains_config: dict):
        cfg = (chains_config or {}).get('solana', {})
        self.rpc = SolanaRPC(cfg.get('rpc_url'), [cfg.get('ankr_rpc')], cfg.get('helius_api_key'))

    async def check(self, token: str, chain: str, coin_signal: Dict) -> Dict:
        token_info = coin_signal.get('token_info') or {}
        mint_auth = token_info.get('mint_authority')
        freeze_auth = token_info.get('freeze_authority')
        if mint_auth is None and freeze_auth is None:
            # not captured at detection (e.g. DexScreener fallback) -> fetch fresh
            res = await self.rpc.call("getAccountInfo", [token, {"encoding": "jsonParsed"}])
            info = ((res or {}).get('value') or {}).get('data', {}).get('parsed', {}).get('info', {})
            if not info:
                return {'available': False, 'score': 50, 'flag': 'NO_DATA', 'details': 'mint account not readable'}
            mint_auth = info.get('mintAuthority')
            freeze_auth = info.get('freezeAuthority')
        if mint_auth or freeze_auth:
            flags = []
            if mint_auth:
                flags.append('mint authority ACTIVE')
            if freeze_auth:
                flags.append('freeze authority ACTIVE')
            return {'available': True, 'score': 5, 'flag': 'MINT_AUTH_ACTIVE', 'details': '; '.join(flags) + ' - hard rug vector'}
        return {'available': True, 'score': 85, 'flag': 'AUTH_REVOKED', 'details': 'mint+freeze authority revoked'}


class SolanaHoldersAgent:
    """REAL holder distribution: top-20 token accounts -> supply share, owner mapping,
    bundle detection (top10 share), creator-wallet holding."""

    def __init__(self, config: dict, chains_config: dict):
        cfg = (chains_config or {}).get('solana', {})
        self.rpc = SolanaRPC(cfg.get('rpc_url'), [cfg.get('ankr_rpc')], cfg.get('helius_api_key'))

    async def check(self, token: str, chain: str, coin_signal: Dict) -> Dict:
        largest = await self.rpc.call("getTokenLargestAccounts", [token, {"commitment": "confirmed"}])
        if not largest or not largest.get('value'):
            return {'available': False, 'score': 50, 'flag': 'NO_DATA', 'details': 'no holder accounts returned'}
        accounts = largest['value']
        supply_res = await self.rpc.call("getTokenSupply", [token])
        supply = None
        if supply_res and supply_res.get('value'):
            supply = float(supply_res['value'].get('uiAmount') or 0)
        if not supply:
            return {'available': False, 'score': 50, 'flag': 'NO_SUPPLY', 'details': 'supply unreadable'}

        balances = [float(a.get('uiAmount') or 0) for a in accounts[:20]]
        top10_pct = sum(balances[:10]) / supply * 100.0
        top1_pct = (balances[0] / supply * 100.0) if balances else 0.0

        creator = coin_signal.get('creator')
        creator_pct = 0.0
        if creator:
            # map top-5 token accounts -> owner wallets (batched)
            addrs = [a.get('address') for a in accounts[:5] if a.get('address')]
            if addrs:
                acc = await self.rpc.call("getMultipleAccounts", [addrs, {"encoding": "jsonParsed"}])
                for acc_info in (acc or {}).get('value') or []:
                    owner = ((acc_info or {}).get('data', {}).get('parsed', {}).get('info', {}) or {}).get('owner', '')
                    amt = float(((acc_info or {}).get('data', {}).get('parsed', {}).get('info', {}) or {}).get('tokenAmount', {}).get('uiAmount') or 0)
                    if owner and owner == creator:
                        creator_pct = amt / supply * 100.0
                        break

        # Scoring: fresh pump.fun tokens have curve+creator concentration by design,
        # so judge on whether distribution is worse than a healthy fresh launch.
        if top10_pct > 55:
            return {'available': True, 'score': 10, 'flag': 'BUNDLED', 'details': f'top10 {top10_pct:.1f}% of supply - bundle pattern', 'top10_pct': round(top10_pct, 1), 'top1_pct': round(top1_pct, 1), 'creator_pct': round(creator_pct, 1)}
        if top10_pct > 35:
            return {'available': True, 'score': 35, 'flag': 'HEAVY_TOP', 'details': f'top10 {top10_pct:.1f}% concentrated', 'top10_pct': round(top10_pct, 1), 'top1_pct': round(top1_pct, 1), 'creator_pct': round(creator_pct, 1)}
        if creator_pct > 10:
            return {'available': True, 'score': 40, 'flag': 'DEV_HOLDING', 'details': f'creator wallet holds {creator_pct:.1f}% of supply', 'top10_pct': round(top10_pct, 1), 'top1_pct': round(top1_pct, 1), 'creator_pct': round(creator_pct, 1)}
        return {'available': True, 'score': 75, 'flag': 'HEALTHY_HOLDERS', 'details': f'top10 {top10_pct:.1f}%, top1 {top1_pct:.1f}%{f", creator {creator_pct:.1f}%" if creator else ""}', 'top10_pct': round(top10_pct, 1), 'top1_pct': round(top1_pct, 1), 'creator_pct': round(creator_pct, 1)}


class SolanaDevAgent:
    """Creator activity history: signatures on the creator wallet + age of activity.
    Serial launchers churn many creations; fresh wallets are normal for memes (not penalized hard)."""

    def __init__(self, config: dict, chains_config: dict):
        cfg = (chains_config or {}).get('solana', {})
        self.rpc = SolanaRPC(cfg.get('rpc_url'), [cfg.get('ankr_rpc')], cfg.get('helius_api_key'))

    async def check(self, token: str, chain: str, coin_signal: Dict) -> Dict:
        creator = coin_signal.get('creator')
        if not creator:
            return {'available': False, 'score': 50, 'flag': 'UNKNOWN_CREATOR', 'details': 'creator not captured (DexScreener fallback path)'}
        sigs = await self.rpc.call("getSignaturesForAddress", [creator, {"limit": 100}])
        if sigs is None:
            return {'available': False, 'score': 50, 'flag': 'RPC_FAIL', 'details': 'getSignaturesForAddress failed'}
        n = len(sigs)
        # window age: newest to oldest blockTime in the sampled window
        times = [s.get('blockTime') for s in sigs if s.get('blockTime')]
        span_days = (max(times) - min(times)) / 86400.0 if len(times) >= 2 else 0.0
        now = time.time()
        last_active_days = (now - max(times)) / 86400.0 if times else None

        if n >= 60:
            return {'available': True, 'score': 15, 'flag': 'SERIAL_CREATOR', 'details': f'{n} txs in sample window (span {span_days:.1f}d) - serial launcher pattern', 'tx_count': n}
        if n >= 25:
            return {'available': True, 'score': 40, 'flag': 'FREQUENT_CREATOR', 'details': f'{n} txs in window (span {span_days:.1f}d)', 'tx_count': n}
        if n <= 6:
            return {'available': True, 'score': 70, 'flag': 'FRESH_CREATOR', 'details': f'{n} txs - fresh wallet (normal for memes)', 'tx_count': n}
        return {'available': True, 'score': 55, 'flag': 'MODERATE_CREATOR', 'details': f'{n} txs in window', 'tx_count': n}

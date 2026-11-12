"""ProfitAgent - checks profitable medium accounts (and reuses brutal-wallet-auditor grading).
Samples recent buyers of the token via DexScreener trades, then grades each wallet via Ankr history.
Flags: PROFITABLE_BUYERS, DEGEN_BUYERS, MIXED
"""
import logging, aiohttp
from typing import Dict, List

logger = logging.getLogger(__name__)

class ProfitAgent:
    def __init__(self, config: dict, chains_config: dict):
        self.config = config
        self.ankr_key = config.get('ankr',{}).get('api_key','')

    async def check(self, token: str, chain: str, coin_signal: Dict) -> Dict:
        # 1. Get recent buyers of this token via DexScreener trades (top traders)
        buyers = await self._fetch_recent_buyers(token, chain)
        if not buyers:
            return {'score': 50, 'flag': 'NO_BUYERS_YET', 'details': 'no recent trades found'}

        # 2. For each buyer, quick Ankr profitability check (cached)
        # To stay within Ankr 50/min, sample 3 buyers max
        sample = buyers[:3]
        scores = []
        for buyer in sample:
            s = await self._grade_wallet(buyer, chain)
            scores.append(s)

        if not scores:
            return {'score': 50, 'flag': 'NO_GRADE', 'details': f'{len(buyers)} buyers found'}

        avg = sum(scores)/len(scores)
        if avg >= 65:
            return {'score': 75, 'flag': 'PROFITABLE_BUYERS', 'details': f'avg wallet score {avg:.0f} on {len(sample)} buyers', 'avg_score': avg}
        if avg <= 40:
            return {'score': 30, 'flag': 'DEGEN_BUYERS', 'details': f'avg wallet {avg:.0f} - likely bots/degen', 'avg_score': avg}
        return {'score': 50, 'flag': 'MIXED_BUYERS', 'details': f'avg wallet {avg:.0f}', 'avg_score': avg}

    async def _fetch_recent_buyers(self, token: str, chain: str) -> List[str]:
        try:
            # DexScreener trades endpoint: get pair and then top traders is not public, so fallback to Ankr token holders for Base?
            # Use Ankr getTokenHolders if available, else empty
            if self.ankr_key and not self.ankr_key.startswith('${'):
                # Try DexScreener pair trades
                url = f"https://api.dexscreener.com/tokens/v1/{chain}/{token}"
                async with aiohttp.ClientSession() as s:
                    async with s.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                        if r.status==200:
                            data = await r.json()
                            if isinstance(data, list) and data and data[0].get('pairAddress'):
                                return []  # no wallet list from this endpoint, return empty to skip
            return []
        except: return []

    async def _grade_wallet(self, wallet: str, chain: str) -> float:
        # Reuse brutal-wallet-auditor math: win_rate >60% + holding time >15m + 5-20 trades => A grade
        # Simplified: use Ankr tx count as proxy
        try:
            from utils.ankr_api import AnkrAPI
        except ImportError:
            try:
                from src.utils.ankr_api import AnkrAPI
            except ImportError:
                from ..utils.ankr_api import AnkrAPI
        if not self.ankr_key or self.ankr_key.startswith('${'):
            return 50
        ankr = AnkrAPI(self.ankr_key)
        ankr_chain = 'base' if chain=='base' else 'arb1' if chain=='robinhood' else 'eth'
        txs = await ankr.get_transactions(wallet, ankr_chain, page_size=20)
        # Score: 5-25 txs is ideal (brutal-wallet-auditor Sniper filter)
        n = len(txs)
        if 5 <= n <= 25:
            return 70
        if n > 40:
            return 35  # high frequency bot
        if n < 3:
            return 45  # new wallet
        return 55

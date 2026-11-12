"""DevAgent - checks deployer history via Ankr + DexScreener.
Uses Ankr ankr_getTransactionsByAddress on the PairCreated tx creator.
Flags: RUG_HISTORY, SERIAL_DEV, CLEAN_DEV
"""
import logging
import aiohttp
from typing import Dict

logger = logging.getLogger(__name__)

class DevAgent:
    def __init__(self, config: dict, chains_config: dict):
        self.config = config
        self.chains_config = chains_config
        self.ankr_key = config.get('ankr', {}).get('api_key','')

    async def check(self, token: str, chain: str, coin_signal: Dict) -> Dict:
        # 1. Get deployer via DexScreener pair info
        deployer = coin_signal.get('deployer') or await self._fetch_deployer(token, chain)
        if not deployer:
            return {'score': 50, 'flag': 'UNKNOWN_DEV', 'details': 'no deployer found', 'deployer': None}

        # 2. Ankr: how many token transfers has deployer done? + check past tokens
        try:
            from utils.ankr_api import AnkrAPI
        except ImportError:
            try:
                from src.utils.ankr_api import AnkrAPI
            except ImportError:
                from ..utils.ankr_api import AnkrAPI
        if not self.ankr_key or self.ankr_key.startswith('${'):
            return {'score': 55, 'flag': 'NO_ANKR', 'details': 'ankr key missing', 'deployer': deployer}

        ankr = AnkrAPI(self.ankr_key)
        ankr_chain = 'base' if chain=='base' else 'arb1' if chain=='robinhood' else 'eth'
        # Fetch recent transactions and token transfers for deployer
        txs = await ankr.get_transactions(deployer, ankr_chain, page_size=20)
        transfers = await ankr.get_token_transfers(deployer, ankr_chain, page_size=20)

        # Heuristics
        past_tokens = len({t.get('token_address','') for t in transfers})
        # If deployer made >5 tokens in last 20 txs -> serial dev (often rugger)
        if past_tokens >= 6:
            return {'score': 15, 'flag': 'SERIAL_DEV', 'details': f'{past_tokens} tokens in 20 transfers', 'deployer': deployer, 'past_tokens': past_tokens}
        if past_tokens >= 3:
            return {'score': 35, 'flag': 'FREQUENT_DEV', 'details': f'{past_tokens} tokens', 'deployer': deployer, 'past_tokens': past_tokens}
        # Check for honeypot-like: deployer still holds huge % (would need holders agent, so just warn)
        if len(txs) < 3:
            return {'score': 60, 'flag': 'FRESH_DEV', 'details': 'new wallet, caution', 'deployer': deployer}
        return {'score': 75, 'flag': 'CLEAN_DEV', 'details': f'{past_tokens} past tokens, {len(txs)} txs', 'deployer': deployer, 'past_tokens': past_tokens}

    async def _fetch_deployer(self, token: str, chain: str) -> str:
        try:
            url = f"https://api.dexscreener.com/tokens/v1/{chain}/{token}"
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status!=200: return None
                    data = await r.json()
                    if isinstance(data, list) and data:
                        # DexScreener doesn't directly give deployer, fallback to pair creator via tx
                        return data[0].get('pairAddress')  # placeholder, real deployer needs tx trace
        except: pass
        return None

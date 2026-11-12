"""KolAgent - checks if known KOLs / smart wallets bought the coin recently.
Reuses tracked_wallets list as KOL set; checks via Ankr token transfers.
Flags: KOL_BUYING, NO_KOL, KOL_DUMPING
"""
import logging
from typing import Dict

logger = logging.getLogger(__name__)

class KolAgent:
    def __init__(self, config: dict, chains_config: dict):
        self.config = config
        self.kols = set([w.lower() for w in config.get('overseer',{}).get('kols', [])] +
                        config.get('scout',{}).get('smart_money',{}).get('tracked_wallets',[]))
        self.ankr_key = config.get('ankr',{}).get('api_key','')

    async def check(self, token: str, chain: str, coin_signal: Dict) -> Dict:
        if not self.kols:
            return {'score': 50, 'flag': 'NO_KOLS_CONFIGURED', 'details': 'add kols to overseer.kols or scout.smart_money.tracked_wallets'}
        if not self.ankr_key or self.ankr_key.startswith('${'):
            return {'score': 50, 'flag': 'NO_ANKR', 'details': 'ankr missing'}
        try:
            from utils.ankr_api import AnkrAPI
        except ImportError:
            try:
                from src.utils.ankr_api import AnkrAPI
            except ImportError:
                from ..utils.ankr_api import AnkrAPI
        ankr = AnkrAPI(self.ankr_key)
        ankr_chain = 'base' if chain=='base' else 'arb1' if chain=='robinhood' else 'eth'
        # Sample first 5 KOLs (avoid 50 req/min limit)
        sample = list(self.kols)[:5]
        hits = 0
        for kol in sample:
            transfers = await ankr.get_token_transfers(kol, ankr_chain, page_size=10)
            for t in transfers:
                if t.get('token_address','').lower() == token.lower() and t.get('to','').lower()==kol.lower():
                    hits += 1
                    break
        if hits >= 2:
            return {'score': 85, 'flag': 'KOL_BUYING', 'details': f'{hits}/{len(sample)} kols bought'}
        if hits == 1:
            return {'score': 65, 'flag': 'ONE_KOL', 'details': '1 kol bought'}
        return {'score': 45, 'flag': 'NO_KOL', 'details': f'0/{len(sample)} kols on this token'}

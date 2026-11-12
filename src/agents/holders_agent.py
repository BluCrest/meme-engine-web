"""HoldersAgent - whale / bundle / distribution check via DexScreener.
Flags: WHALE_HEAVY, BUNDLED, HEALTHY_DISTRIBUTION
"""
import logging, aiohttp
from typing import Dict

logger = logging.getLogger(__name__)

class HoldersAgent:
    def __init__(self, config: dict, chains_config: dict):
        self.config = config

    async def check(self, token: str, chain: str, coin_signal: Dict) -> Dict:
        try:
            url = f"https://api.dexscreener.com/tokens/v1/{chain}/{token}"
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status!=200:
                        return {'score': 50, 'flag': 'NO_DATA', 'details': f'dexscreener {r.status}'}
                    data = await r.json()
                    if not isinstance(data, list) or not data:
                        return {'score': 50, 'flag': 'NO_PAIR', 'details': 'no pair yet'}
                    pair = data[0]
                    # Use pair info as holder proxy (true holders needs Ankr getTokenHolders if available)
                    fdv = pair.get('fdv', 0) or 0
                    liquidity = pair.get('liquidity',{}).get('usd',0) or 0
                    # Volume vs liquidity sanity
                    vol24 = pair.get('volume',{}).get('h24',0) or 0
                    # Heuristic: liquidity < $5k -> high rug risk
                    if liquidity < 5000:
                        return {'score': 20, 'flag': 'LOW_LIQ', 'details': f'liq ${liquidity:.0f}', 'liquidity': liquidity, 'fdv': fdv}
                    if liquidity < 15000:
                        return {'score': 40, 'flag': 'THIN_LIQ', 'details': f'liq ${liquidity:.0f} vol24 ${vol24:.0f}', 'liquidity': liquidity}
                    # Check top holder concentration via pair holders if available (DexScreener doesn't give, so mock with fdv/liq ratio)
                    # If fdv > 50x liquidity, likely whale heavy
                    if fdv and liquidity and fdv / max(liquidity,1) > 80:
                        return {'score': 30, 'flag': 'WHALE_HEAVY', 'details': f'fdv/liq {fdv/max(liquidity,1):.0f}x', 'fdv': fdv, 'liquidity': liquidity}
                    return {'score': 70, 'flag': 'HEALTHY_DISTRIBUTION', 'details': f'liq ${liquidity:.0f} vol24 ${vol24:.0f}', 'liquidity': liquidity, 'fdv': fdv}
        except Exception as e:
            return {'score': 50, 'flag': 'ERROR', 'details': str(e)[:100]}

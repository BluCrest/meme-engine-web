import aiohttp
import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class GoldRushAPI:
    """Covalent GoldRush API for Robinhood Chain (robinhood-mainnet, chain 4663).
    Auth: ?key=<GOLDRUSH_API_KEY>  Docs: https://goldrush.dev/docs/chains/robinhood-chain
    """
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base = "https://api.covalenthq.com/v1"

    def _url(self, chain: str, path: str) -> str:
        # chain must be robinhood-mainnet (or eth-mainnet, base-mainnet etc.)
        return f"{self.base}/{chain}/{path}?key={self.api_key}"

    async def get_balances(self, address: str, chain: str = "robinhood-mainnet") -> List[Dict]:
        url = self._url(chain, f"address/{address}/balances_v2/")
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        logger.debug(f"GoldRush balances {r.status}")
                        return []
                    data = await r.json()
                    return data.get('data', {}).get('items', [])
        except Exception as e:
            logger.debug(f"GoldRush balances error: {e}")
            return []

    async def get_transactions(self, address: str, chain: str = "robinhood-mainnet", page_size: int = 10) -> List[Dict]:
        url = self._url(chain, f"address/{address}/transactions_v3/") + f"&page-size={page_size}"
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        return []
                    data = await r.json()
                    return data.get('data', {}).get('items', [])
        except Exception as e:
            logger.debug(f"GoldRush tx error: {e}")
            return []

    async def get_token_transfers(self, address: str, chain: str = "robinhood-mainnet", page_size: int = 10) -> List[Dict]:
        # GoldRush has no direct token_transfers filter, use transactions and parse log_events
        # For now reuse get_transactions and extract transfers from log_events
        txs = await self.get_transactions(address, chain, page_size)
        transfers = []
        for tx in txs:
            for log in tx.get('log_events', []):
                if log.get('decoded') and 'Transfer' in str(log.get('decoded', {}).get('name','')):
                    transfers.append(log)
        return transfers

    async def get_logs(self, chain: str, contract: str, topic: str, from_block: int, to_block: int) -> List[Dict]:
        url = self._url(chain, f"events/address/{contract}/") + f"&starting-block={from_block}&ending-block={to_block}"
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        return []
                    data = await r.json()
                    return data.get('data', {}).get('items', [])
        except Exception as e:
            logger.debug(f"GoldRush logs error: {e}")
            return []

    async def get_logs_by_topic(self, chain: str, topic: str, from_block: int, to_block: int) -> List[Dict]:
        # GoldRush topic hash endpoint: /v1/{chain}/events/topics/{topic}/
        url = self._url(chain, f"events/topics/{topic}/") + f"&starting-block={from_block}&ending-block={to_block}"
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        logger.debug(f"GoldRush topic logs {r.status}")
                        return []
                    data = await r.json()
                    return data.get('data', {}).get('items', [])
        except Exception as e:
            logger.debug(f"GoldRush topic logs error: {e}")
            return []

    async def get_new_pairs(self, chain: str, factory: str, from_block: int, to_block: int) -> List[Dict]:
        topic = "0x783cca1c0412dd0d695e71d7c3c031203101cd3024dc4ed9507039e4d38d5f8d"
        return await self.get_logs(chain, factory, topic, from_block, to_block)

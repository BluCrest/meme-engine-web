import aiohttp
import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class AnkrAPI:
    """Ankr Advanced API client for multi-chain data"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = f"https://rpc.ankr.com/multichain/{api_key}"
        self.chain_map = {
            'ethereum': 'eth',
            'base': 'base',
            'arbitrum': 'arbitrum',
            'polygon': 'polygon',
            'bsc': 'bsc',
            'avalanche': 'avalanche',
            'optimism': 'optimism'
        }
    
    async def _post(self, method: str, params: dict) -> Optional[dict]:
        """Make a JSON-RPC call to Ankr"""
        try:
            payload = {
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "id": 1
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(self.base_url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.error(f"Ankr API error: {resp.status}")
                        return None
                    
                    data = await resp.json()
                    
                    if 'error' in data:
                        logger.error(f"Ankr API error: {data['error']}")
                        return None
                    
                    return data.get('result')
                    
        except Exception as e:
            logger.error(f"Ankr API call failed: {e}")
            return None
    
    async def get_transactions(self, address: str, chain: str = 'eth', 
                                page_size: int = 50) -> List[Dict]:
        """Get recent transactions for a wallet"""
        ankr_chain = self.chain_map.get(chain, chain)
        
        result = await self._post("ankr_getTransactionsByAddress", {
            "address": address,
            "blockchain": ankr_chain,
            "pageSize": page_size
        })
        
        if not result or 'transactions' not in result:
            return []
        
        transactions = []
        for tx in result['transactions']:
            transactions.append({
                'hash': tx.get('transactionHash', ''),
                'from': tx.get('from', ''),
                'to': tx.get('to', ''),
                'value': self._hex_to_int(tx.get('value', '0x0')),
                'gas': self._hex_to_int(tx.get('gas', '0x0')),
                'gas_price': self._hex_to_int(tx.get('gasPrice', '0x0')),
                'timestamp': self._parse_timestamp(tx.get('blockTimestamp', '0')),
                'block': self._hex_to_int(tx.get('blockNumber', '0x0')),
                'success': tx.get('success', True)
            })
        
        return transactions
    
    async def get_token_transfers(self, address: str, chain: str = 'eth',
                                   page_size: int = 50) -> List[Dict]:
        """Get token transfer history for a wallet"""
        ankr_chain = self.chain_map.get(chain, chain)
        
        result = await self._post("ankr_getTokenTransfersByAddress", {
            "address": address,
            "blockchain": ankr_chain,
            "pageSize": page_size
        })
        
        if not result or 'tokenTransfers' not in result:
            return []
        
        transfers = []
        for transfer in result['tokenTransfers']:
            transfers.append({
                'hash': transfer.get('transactionHash', ''),
                'from': transfer.get('from', ''),
                'to': transfer.get('to', ''),
                'value': transfer.get('value', '0'),
                'token_address': transfer.get('contractAddress', ''),
                'token_symbol': transfer.get('tokenSymbol', 'UNKNOWN'),
                'token_name': transfer.get('tokenName', 'Unknown'),
                'token_decimals': int(transfer.get('tokenDecimals', '18')),
                'timestamp': self._parse_timestamp(transfer.get('blockTimestamp', '0'))
            })
        
        return transfers
    
    async def get_token_balance(self, address: str, chain: str = 'eth',
                                 token_address: str = None) -> Dict:
        """Get token balance(s) for a wallet"""
        ankr_chain = self.chain_map.get(chain, chain)
        
        params = {
            "walletAddress": address,
            "blockchain": ankr_chain
        }
        
        if token_address:
            params["contractAddresses"] = [token_address]
        
        result = await self._post("ankr_getAccountBalance", params)
        
        if not result or 'balances' not in result:
            return {}
        
        balances = {}
        for balance in result['balances']:
            token_addr = balance.get('contractAddress', '')
            balances[token_addr] = {
                'balance': balance.get('balance', '0'),
                'balance_usd': float(balance.get('balanceUsd', '0')),
                'token_symbol': balance.get('tokenSymbol', 'UNKNOWN'),
                'token_name': balance.get('tokenName', 'Unknown'),
                'token_decimals': int(balance.get('tokenDecimals', '18'))
            }
        
        return balances
    
    async def get_token_price(self, chain: str, token_address: str) -> float:
        """Get current token price in USD"""
        ankr_chain = self.chain_map.get(chain, chain)
        
        result = await self._post("ankr_getTokenPrice", {
            "blockchain": ankr_chain,
            "contractAddress": token_address
        })
        
        if result and 'usdPrice' in result:
            return float(result['usdPrice'])
        
        return 0.0
    
    async def get_wallet_balance_usd(self, address: str, chain: str = 'eth') -> float:
        """Get total wallet balance in USD"""
        balances = await self.get_token_balance(address, chain)
        return sum(b.get('balance_usd', 0) for b in balances.values())
    
    async def get_new_pairs(self, chain: str = 'eth', page_size: int = 20) -> List[Dict]:
        """Get newly created token pairs (EVM chains only)"""
        ankr_chain = self.chain_map.get(chain, chain)
        
        result = await self._post("ankr_getTokenHolders", {
            "blockchain": ankr_chain,
            "pageSize": page_size,
            "pageToken": ""
        })
        
        # Note: Ankr may not have a direct "new pairs" endpoint
        # This is a placeholder - use DexScreener for new pair detection
        return []
    
    async def get_wallet_history(self, address: str, chain: str = 'eth',
                                  from_block: int = 0, to_block: int = 99999999) -> Dict:
        """Get comprehensive wallet history"""
        transactions = await self.get_transactions(address, chain)
        token_transfers = await self.get_token_transfers(address, chain)
        balances = await self.get_token_balance(address, chain)
        
        return {
            'address': address,
            'chain': chain,
            'transactions': transactions,
            'token_transfers': token_transfers,
            'balances': balances,
            'total_tx_count': len(transactions),
            'total_transfer_count': len(token_transfers)
        }
    
    async def bulk_wallet_scan(self, addresses: List[str], chain: str = 'eth') -> Dict:
        """Scan multiple wallets efficiently"""
        results = {}
        
        for address in addresses:
            history = await self.get_wallet_history(address, chain)
            results[address] = history
        
        return results
    
    def _hex_to_int(self, val) -> int:
        """Convert hex string to integer"""
        if isinstance(val, str):
            if val.startswith('0x'):
                try:
                    return int(val, 16)
                except ValueError:
                    return 0
            try:
                return int(val)
            except ValueError:
                return 0
        return int(val) if val else 0
    
    def _parse_timestamp(self, ts) -> str:
        """Parse timestamp to ISO format"""
        try:
            ts_int = self._hex_to_int(ts)
            if ts_int > 1e12:
                ts_int = ts_int / 1000
            dt = datetime.fromtimestamp(ts_int)
            return dt.isoformat()
        except:
            return str(ts)

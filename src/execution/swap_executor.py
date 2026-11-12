import asyncio
import logging
import time
from typing import Dict, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

class SwapExecutor:
    """Executes buy/sell swaps across multiple chains"""
    
    def __init__(self, config: dict):
        self.config = config.get('execution', {})
        self.default_buy_amount = self.config.get('default_buy_amount_usd', 100)
        self.max_buy_amount = self.config.get('max_buy_amount_usd', 500)
        self.max_slippage = self.config.get('max_slippage_percent', 5)
        self.retry_attempts = self.config.get('retry_attempts', 3)
        self.retry_delay = self.config.get('retry_delay_seconds', 2)
    
    async def buy_token(self, token_address: str, chain: str, amount_usd: float,
                        chains_config: dict) -> Optional[Dict]:
        """Execute a token buy across chains"""
        amount_usd = min(amount_usd, self.max_buy_amount)
        
        logger.info(f"Buying {token_address} on {chain} for ${amount_usd:.2f}")
        
        for attempt in range(self.retry_attempts):
            try:
                if chain == 'solana':
                    return await self._buy_solana(token_address, amount_usd, chains_config['solana'])
                elif chain in ('base', 'robinhood'):
                    return await self._buy_evm(token_address, chain, amount_usd, chains_config.get(chain, {}))
                else:
                    logger.error(f"Unsupported chain: {chain}")
                    return None
                    
            except Exception as e:
                logger.error(f"Buy attempt {attempt + 1} failed: {e}")
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(self.retry_delay)
        
        logger.error(f"All buy attempts failed for {token_address} on {chain}")
        return None
    
    async def sell_token(self, token_address: str, chain: str, token_amount: float,
                         sell_percent: float, chains_config: dict) -> Optional[Dict]:
        """Execute a token sell across chains"""
        actual_amount = token_amount * (sell_percent / 100)
        
        logger.info(f"Selling {sell_percent}% of {token_address} on {chain}")
        
        for attempt in range(self.retry_attempts):
            try:
                if chain == 'solana':
                    return await self._sell_solana(token_address, actual_amount, chains_config['solana'])
                elif chain in ('base', 'robinhood'):
                    return await self._sell_evm(token_address, chain, actual_amount, chains_config.get(chain, {}))
                else:
                    return None
                    
            except Exception as e:
                logger.error(f"Sell attempt {attempt + 1} failed: {e}")
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(self.retry_delay)
        
        return None
    
    async def _buy_solana(self, token_address: str, amount_usd: float, chain_config: dict) -> Dict:
        """Buy token on Solana using Jupiter Aggregator"""
        try:
            import aiohttp
            from solders.keypair import Keypair
            from solders.transaction import VersionedTransaction
            import base64
            
            jupiter_api = chain_config.get('jupiter_api', 'https://quote-api.jup.ag/v6')
            
            # SOL mint address
            sol_mint = 'So11111111111111111111111111111111111111112'
            
            # Get quote
            quote_url = f"{jupiter_api}/quote"
            quote_params = {
                'inputMint': sol_mint,
                'outputMint': token_address,
                'amount': int(amount_usd * 1e9),  # lamports
                'slippageBps': int(self.max_slippage * 100)
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(quote_url, params=quote_params) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        raise Exception(f"Jupiter quote failed: {error_text}")
                    
                    quote = await resp.json()
                    
                    # Get swap transaction
                    swap_url = f"{jupiter_api}/swap"
                    swap_body = {
                        'quoteResponse': quote,
                        'userPublicKey': chain_config.get('public_key', ''),
                        'wrapAndUnwrapSol': True
                    }
                    
                    async with session.post(swap_url, json=swap_body) as resp:
                        if resp.status != 200:
                            error_text = await resp.text()
                            raise Exception(f"Jupiter swap failed: {error_text}")
                        
                        swap_data = await resp.json()
                        
                        # Sign and send transaction
                        swap_tx = swap_data.get('swapTransaction')
                        if not swap_tx:
                            raise Exception("No swap transaction returned")
                        
                        # Decode and sign
                        private_key = chain_config.get('private_key')
                        if not private_key:
                            raise Exception("No private key configured for Solana")
                        
                        keypair = Keypair.from_base58_string(private_key)
                        tx_bytes = base64.b64decode(swap_tx)
                        tx = VersionedTransaction.from_bytes(tx_bytes)
                        
                        signed_tx = VersionedTransaction(tx.message, [keypair])
                        
                        # Send transaction
                        send_url = f"{chain_config.get('rpc_url')}"
                        send_payload = {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "sendTransaction",
                            "params": [
                                base64.b64encode(bytes(signed_tx)).decode(),
                                {"encoding": "base64", "skipPreflight": True}
                            ]
                        }
                        
                        async with session.post(send_url, json=send_payload) as resp:
                            result = await resp.json()
                            tx_hash = result.get('result', '')
                            
                            if not tx_hash:
                                raise Exception(f"Transaction failed: {result}")
                            
                            return {
                                'success': True,
                                'tx_hash': tx_hash,
                                'chain': 'solana',
                                'amount_usd': amount_usd,
                                'token_address': token_address,
                                'timestamp': datetime.now().isoformat(),
                                'quote': {
                                    'in_amount': quote.get('inAmount'),
                                    'out_amount': quote.get('outAmount'),
                                    'price_impact': quote.get('priceImpactPct')
                                }
                            }
                            
        except ImportError:
            raise Exception("Missing Solana dependencies: pip install solders")
        except Exception as e:
            logger.error(f"Solana buy error: {e}")
            raise
    
    async def _sell_solana(self, token_address: str, token_amount: float, chain_config: dict) -> Dict:
        """Sell token on Solana using Jupiter"""
        try:
            import aiohttp
            from solders.keypair import Keypair
            from solders.transaction import VersionedTransaction
            import base64
            
            jupiter_api = chain_config.get('jupiter_api', 'https://quote-api.jup.ag/v6')
            sol_mint = 'So11111111111111111111111111111111111111112'
            
            # Get token decimals (simplified - should fetch from chain)
            decimals = 9
            
            quote_url = f"{jupiter_api}/quote"
            quote_params = {
                'inputMint': token_address,
                'outputMint': sol_mint,
                'amount': int(token_amount * (10 ** decimals)),
                'slippageBps': int(self.max_slippage * 100)
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(quote_url, params=quote_params) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        raise Exception(f"Jupiter quote failed: {error_text}")
                    
                    quote = await resp.json()
                    
                    # Similar swap flow as buy
                    swap_url = f"{jupiter_api}/swap"
                    swap_body = {
                        'quoteResponse': quote,
                        'userPublicKey': chain_config.get('public_key', ''),
                        'wrapAndUnwrapSol': True
                    }
                    
                    async with session.post(swap_url, json=swap_body) as resp:
                        if resp.status != 200:
                            error_text = await resp.text()
                            raise Exception(f"Jupiter swap failed: {error_text}")
                        
                        swap_data = await resp.json()
                        swap_tx = swap_data.get('swapTransaction')
                        
                        private_key = chain_config.get('private_key')
                        keypair = Keypair.from_base58_string(private_key)
                        tx_bytes = base64.b64decode(swap_tx)
                        tx = VersionedTransaction.from_bytes(tx_bytes)
                        signed_tx = VersionedTransaction(tx.message, [keypair])
                        
                        send_payload = {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "sendTransaction",
                            "params": [
                                base64.b64encode(bytes(signed_tx)).decode(),
                                {"encoding": "base64", "skipPreflight": True}
                            ]
                        }
                        
                        async with session.post(chain_config['rpc_url'], json=send_payload) as resp:
                            result = await resp.json()
                            tx_hash = result.get('result', '')
                            
                            return {
                                'success': True,
                                'tx_hash': tx_hash,
                                'chain': 'solana',
                                'token_address': token_address,
                                'token_amount': token_amount,
                                'timestamp': datetime.now().isoformat(),
                                'quote': {
                                    'out_amount': quote.get('outAmount'),
                                    'price_impact': quote.get('priceImpactPct')
                                }
                            }
                            
        except Exception as e:
            logger.error(f"Solana sell error: {e}")
            raise
    
    async def _buy_evm(self, token_address: str, chain: str, amount_usd: float, chain_config: dict) -> Dict:
        """Buy token on EVM chains using 1inch or Uniswap"""
        try:
            import aiohttp
            from web3 import Web3
            
            one_inch_api = chain_config.get('one_inch_api')
            weth_addresses = {
                'base': '0x4200000000000000000000000000000000000006',
                'robinhood': '0x4200000000000000000000000000000000000006'
            }
            
            weth = weth_addresses.get(chain, weth_addresses['base'])
            private_key = chain_config.get('private_key')
            
            if not private_key:
                raise Exception(f"No private key configured for {chain}")
            
            w3 = Web3(Web3.HTTPProvider(chain_config['rpc_url']))
            wallet_address = w3.eth.account.from_key(private_key).address
            
            if one_inch_api:
                # Use 1inch API
                api_key = chain_config.get('one_inch_api_key', '')
                
                # Get quote
                quote_url = f"{one_inch_api}/quote"
                params = {
                    'src': weth,
                    'dst': token_address,
                    'amount': str(int(amount_usd * 1e18))
                }
                
                headers = {'Authorization': f'Bearer {api_key}'}
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(quote_url, params=params, headers=headers) as resp:
                        if resp.status != 200:
                            raise Exception("1inch quote failed")
                        quote = await resp.json()
                    
                    # Get swap calldata
                    swap_url = f"{one_inch_api}/swap"
                    params['fromAddress'] = wallet_address
                    params['slippage'] = self.max_slippage
                    
                    async with session.get(swap_url, params=params, headers=headers) as resp:
                        if resp.status != 200:
                            raise Exception("1inch swap failed")
                        swap_data = await resp.json()
                    
                    # Sign and send - Phase1 priority fee for next-block landing (GoldRush/Ankr eth_maxPriorityFeePerGas)
                    tx = swap_data.get('tx')
                    # EIP-1559 priority fee calc
                    try:
                        base_fee = w3.eth.get_block('latest').get('baseFeePerGas', w3.to_wei(1, 'gwei'))
                        priority = w3.eth.max_priority_fee if hasattr(w3.eth, 'max_priority_fee') else w3.to_wei(2, 'gwei')
                        max_priority = int(priority * 1.2)  # 20% bump for next block
                        max_fee = int(base_fee * 1.25 + max_priority)
                    except:
                        max_priority = w3.to_wei(2, 'gwei')
                        max_fee = w3.to_wei(30, 'gwei')
                    signed_tx = w3.eth.account.sign_transaction({
                        'from': wallet_address,
                        'to': Web3.to_checksum_address(tx['to']),
                        'value': int(tx['value'], 16) if tx.get('value') else 0,
                        'data': bytes.fromhex(tx['data'][2:] if tx['data'].startswith('0x') else tx['data']),
                        'gas': int(tx.get('gas', 250000)),
                        'maxFeePerGas': max_fee,
                        'maxPriorityFeePerGas': max_priority,
                        'nonce': w3.eth.get_transaction_count(wallet_address),
                        'chainId': w3.eth.chain_id,
                        'type': 2
                    }, private_key)
                    
                    tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
                    
                    return {
                        'success': True,
                        'tx_hash': tx_hash.hex(),
                        'chain': chain,
                        'amount_usd': amount_usd,
                        'token_address': token_address,
                        'timestamp': datetime.now().isoformat()
                    }
            else:
                raise Exception(f"No DEX API configured for {chain}")
                
        except ImportError:
            raise Exception("Missing web3 dependency: pip install web3")
        except Exception as e:
            logger.error(f"EVM buy error for {chain}: {e}")
            raise
    
    async def _sell_evm(self, token_address: str, chain: str, token_amount: float, chain_config: dict) -> Dict:
        """Sell token on EVM chains"""
        # Similar to buy but reversed token flow
        # Simplified - in production would need token approval and exact input swap
        try:
            import aiohttp
            from web3 import Web3
            
            one_inch_api = chain_config.get('one_inch_api')
            weth_addresses = {
                'base': '0x4200000000000000000000000000000000000006',
                'robinhood': '0x4200000000000000000000000000000000000006'
            }
            
            weth = weth_addresses.get(chain, weth_addresses['base'])
            private_key = chain_config.get('private_key')
            
            w3 = Web3(Web3.HTTPProvider(chain_config['rpc_url']))
            wallet_address = w3.eth.account.from_key(private_key).address
            
            if one_inch_api:
                api_key = chain_config.get('one_inch_api_key', '')
                headers = {'Authorization': f'Bearer {api_key}'}
                
                # Get token decimals
                erc20_abi = [{"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"}]
                contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=erc20_abi)
                decimals = contract.functions.decimals().call()
                
                amount_raw = int(token_amount * (10 ** decimals))
                
                async with aiohttp.ClientSession() as session:
                    # Get quote
                    quote_url = f"{one_inch_api}/quote"
                    params = {
                        'src': token_address,
                        'dst': weth,
                        'amount': str(amount_raw)
                    }
                    
                    async with session.get(quote_url, params=params, headers=headers) as resp:
                        quote = await resp.json()
                    
                    # Get swap
                    swap_url = f"{one_inch_api}/swap"
                    params['fromAddress'] = wallet_address
                    params['slippage'] = self.max_slippage
                    
                    async with session.get(swap_url, params=params, headers=headers) as resp:
                        swap_data = await resp.json()
                    
                    tx = swap_data.get('tx')
                    try:
                        base_fee = w3.eth.get_block('latest').get('baseFeePerGas', w3.to_wei(1, 'gwei'))
                        priority = w3.eth.max_priority_fee if hasattr(w3.eth, 'max_priority_fee') else w3.to_wei(2, 'gwei')
                        max_priority = int(priority * 1.2)
                        max_fee = int(base_fee * 1.25 + max_priority)
                    except:
                        max_priority = w3.to_wei(2, 'gwei')
                        max_fee = w3.to_wei(30, 'gwei')
                    signed_tx = w3.eth.account.sign_transaction({
                        'from': wallet_address,
                        'to': Web3.to_checksum_address(tx['to']),
                        'value': int(tx.get('value', '0x0'), 16) if tx.get('value') else 0,
                        'data': bytes.fromhex(tx['data'][2:] if tx['data'].startswith('0x') else tx['data']),
                        'gas': int(tx.get('gas', 250000)),
                        'maxFeePerGas': max_fee,
                        'maxPriorityFeePerGas': max_priority,
                        'nonce': w3.eth.get_transaction_count(wallet_address),
                        'chainId': w3.eth.chain_id,
                        'type': 2
                    }, private_key)
                    
                    tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
                    
                    return {
                        'success': True,
                        'tx_hash': tx_hash.hex(),
                        'chain': chain,
                        'token_address': token_address,
                        'token_amount': token_amount,
                        'timestamp': datetime.now().isoformat()
                    }
                    
        except Exception as e:
            logger.error(f"EVM sell error for {chain}: {e}")
            raise
    
    async def get_token_price(self, token_address: str, chain: str, chains_config: dict) -> float:
        """Get current token price in USD (live DexScreener)"""
        data = await self.get_token_live_data(token_address, chain, chains_config)
        return data.get('priceUsd', 0.0)

    async def get_token_live_data(self, token_address: str, chain: str, chains_config: dict) -> dict:
        """Get live price + mcap + liquidity + volume from DexScreener (not synthetic). Stamped to journal."""
        try:
            import aiohttp
            # DexScreener live: priceUsd, fdv/marketCap, liquidity, volume
            url = f"https://api.dexscreener.com/tokens/v1/{chain}/{token_address}"
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status == 200:
                        data = await r.json()
                        if isinstance(data, list) and data:
                            p = data[0]
                            return {
                                'priceUsd': float(p.get('priceUsd', 0) or 0),
                                'mcap': float(p.get('marketCap', 0) or p.get('fdv', 0) or 0),
                                'fdv': float(p.get('fdv', 0) or 0),
                                'liquidityUsd': float(p.get('liquidity', {}).get('usd', 0) or 0) if isinstance(p.get('liquidity'), dict) else float(p.get('liquidity', 0) or 0),
                                'volume24h': float(p.get('volume', {}).get('h24', 0) or 0) if isinstance(p.get('volume'), dict) else 0,
                                'pairAddress': p.get('pairAddress', ''),
                                'symbol': p.get('baseToken', {}).get('symbol', p.get('symbol', '')) if isinstance(p.get('baseToken'), dict) else p.get('symbol','')
                            }
            return {'priceUsd': 0.0, 'mcap': 0.0, 'fdv': 0.0, 'liquidityUsd': 0.0, 'volume24h': 0.0}
        except Exception as e:
            logger.debug(f"Live data fetch error {chain} {token_address[:8]}: {e}")
            return {'priceUsd': 0.0, 'mcap': 0.0, 'fdv': 0.0, 'liquidityUsd': 0.0, 'volume24h': 0.0}
    
    async def get_token_balance(self, token_address: str, chain: str, chains_config: dict) -> float:
        """Get current token balance in wallet"""
        try:
            chain_config = chains_config.get(chain, {})
            
            if chain == 'solana':
                import aiohttp
                
                rpc_url = chain_config.get('rpc_url')
                public_key = chain_config.get('public_key')
                
                if not public_key:
                    return 0.0
                
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTokenAccountsByOwner",
                    "params": [
                        public_key,
                        {"mint": token_address},
                        {"encoding": "jsonParsed"}
                    ]
                }
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(rpc_url, json=payload) as resp:
                        data = await resp.json()
                        accounts = data.get('result', {}).get('value', [])
                        
                        if accounts:
                            amount = accounts[0]['account']['data']['parsed']['info']['tokenAmount']['amount']
                            decimals = accounts[0]['account']['data']['parsed']['info']['tokenAmount']['decimals']
                            return float(amount) / (10 ** decimals)
                
            else:
                from web3 import Web3
                
                w3 = Web3(Web3.HTTPProvider(chain_config['rpc_url']))
                private_key = chain_config.get('private_key')
                wallet = w3.eth.account.from_key(private_key).address
                
                erc20_abi = [
                    {"constant": True, "inputs": [{"name": "_owner", "type": "address"}], 
                     "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}], "type": "function"},
                    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"}
                ]
                
                contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=erc20_abi)
                balance = contract.functions.balanceOf(Web3.to_checksum_address(wallet)).call()
                decimals = contract.functions.decimals().call()
                
                return balance / (10 ** decimals)
                
        except Exception as e:
            logger.debug(f"Balance check error: {e}")
            return 0.0

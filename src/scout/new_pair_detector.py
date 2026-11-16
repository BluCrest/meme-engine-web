import asyncio
import json
import logging
import time
from typing import Callable, Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class NewPairDetector:
    """Detects newly created liquidity pairs across chains"""
    
    def __init__(self, config: dict, callback: Callable):
        self.config = config.get('scout', {}).get('new_pair', {})
        self.callback = callback
        self.running = False
        self.seen_tokens = set()
        self.min_liquidity = self.config.get('min_liquidity_usd', 5000)
        self.max_buy_tax = self.config.get('max_buy_tax', 10)
        self.max_sell_tax = self.config.get('max_sell_tax', 10)
        self._tasks: list = []
    
    async def start(self, chains_config: dict):
        """Start monitoring all enabled chains"""
        self.running = True
        self._tasks = []
        
        if chains_config.get('solana', {}).get('enabled'):
            self._tasks.append(asyncio.create_task(self._monitor_solana_pumpfun(chains_config['solana'])))
        
        if chains_config.get('base', {}).get('enabled'):
            self._tasks.append(asyncio.create_task(self._monitor_uniswap_v3(chains_config['base'], 'base')))
        
        if chains_config.get('robinhood', {}).get('enabled'):
            # Robinhood uses GoldRush (Ankr has no robinhood-mainnet)
            self._tasks.append(asyncio.create_task(self._monitor_robinhood_goldrush(chains_config['robinhood'])))
            # fallback also try Ankr if goldrush fails
    
    async def stop(self):
        self.running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
    
    async def _monitor_robinhood_goldrush(self, chain_config: dict):
        """Phase1 Scanner: GoldRush minute-level PairCreated (robinhood 4663) via logs + DexScreener fallback. Heartbeat 30s."""
        import aiohttp
        logger.info("Monitoring Robinhood Chain via GoldRush logs PairCreated 0x783cca1... (robinhood-mainnet 4663) for new pairs every minute...")
        heartbeat = time.time()
        poll_count = 0
        last_block = 0
        try:
            async with aiohttp.ClientSession() as s:
                rpc_url = "https://rpc.goldrushdata.com/v1/robinhood-mainnet"
                payload = {"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}
                goldrush_key = self.config.get('goldrush',{}).get('api_key','')
                async with s.post(rpc_url, json=payload, headers={"Authorization": f"Bearer {goldrush_key}"}, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status == 200:
                        j = await r.json()
                        last_block = int(j.get('result','0x0'), 16) if j.get('result') else 0
                        logger.info(f"Robinhood tip block {last_block} (GoldRush RPC)")
        except: pass
        if last_block == 0:
            last_block = 48540000

        while self.running:
            try:
                poll_count += 1
                # --- 1. Force advance block each poll + try GoldRush logs by topic ---
                found_logs = 0
                cur_block = last_block + 20  # always advance forward 20 blocks
                logs = []
                api = None
                # Get clean key (strip ${} template if present)
                goldrush_key = self.config.get('goldrush',{}).get('api_key','')
                actual_key = goldrush_key
                if goldrush_key and goldrush_key.startswith('${') and goldrush_key.endswith('}'):
                    actual_key = goldrush_key[2:-1]
                if actual_key:
                    try:
                        from utils.goldrush_api import GoldRushAPI
                        api = GoldRushAPI(actual_key)
                        # Scan the forced-advance range for PairCreated logs
                        chunk_start = last_block + 1
                        chunk_end = min(chunk_start + 99, cur_block)
                        if chunk_start <= chunk_end:
                            logs = await api.get_logs_by_topic("robinhood-mainnet", "0x783cca1c0412dd0d695e71d7c3c031203101cd3024dc4ed9507039e4d38d5f8d", chunk_start, chunk_end)
                    except Exception as ge:
                        logger.debug(f"GoldRush logs error: {ge}")
                        logs = []
                # Parse each log for token addresses
                found_logs = 0
                if logs:
                    for log in logs:
                        try:
                            topics = log.get('raw_log_topics', []) or log.get('topics', [])
                            if len(topics) >= 3:
                                t0 = '0x' + topics[1][-40:] if isinstance(topics[1], str) else ''
                                t1 = '0x' + topics[2][-40:] if isinstance(topics[2], str) else ''
                                weth_like = '0x117cc2133c37B721F49dE2A7a74833232B3B4C0C'.lower()
                                new_tok = None
                                if t0.lower() != weth_like and t0.lower() != '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee':
                                    new_tok = t0
                                elif t1.lower() != weth_like:
                                    new_tok = t1
                                if new_tok and new_tok.lower() not in self.seen_tokens:
                                    self.seen_tokens.add(new_tok.lower())
                                    sig = {
                                        'token_address': new_tok,
                                        'chain': 'robinhood',
                                        'source': 'goldrush_logs',
                                        'score': 70,
                                        'price': 0.0001,
                                        'liquidity_usd': 0,
                                        'detected_at': datetime.now().isoformat(),
                                        'pair_address': log.get('address','')
                                    }
                                    logger.info(f"New Robinhood pair via GoldRush logs: {new_tok} block {cur_block} (poll {poll_count})")
                                    await self.callback(sig)
                                    found_logs += 1
                        except Exception as e:
                            logger.debug(f"GoldRush log parse error: {e}")
                # --- 2. DexScreener token-boosts fallback (catch any new token, no age filter) ---
                found_dex = 0
                async with aiohttp.ClientSession() as s:
                    try:
                        url2 = "https://api.dexscreener.com/token-boosts/latest/v1"
                        async with s.get(url2, timeout=aiohttp.ClientTimeout(total=10)) as r:
                            if r.status == 200:
                                data = await r.json()
                                for t in data[:20]:
                                    if t.get('chainId') == 'robinhood':
                                        token = t.get('tokenAddress','')
                                        if token and token.lower() not in self.seen_tokens:
                                            try:
                                                async with s.get(f"https://api.dexscreener.com/tokens/v1/robinhood/{token}", timeout=aiohttp.ClientTimeout(total=5)) as pr:
                                                    if pr.status == 200:
                                                        pd = await pr.json()
                                                        if isinstance(pd, list) and pd:
                                                            pass  # any pairCreatedAt is fine - catch all
                                            except: pass
                                            self.seen_tokens.add(token.lower())
                                            sig = {
                                                'token_address': token,
                                                'chain': 'robinhood',
                                                'source': 'dexscreener_robinhood',
                                                'score': 65,
                                                'price': 0.0001,
                                                'liquidity_usd': 0,
                                                'detected_at': datetime.now().isoformat(),
                                                'pair_address': t.get('pairAddress',''),
                                                'token_info': {'symbol': t.get('symbol','???'), 'name': t.get('name','Unknown')}
                                            }
                                            logger.info(f"New Robinhood pair via DexScreener: {token} {t.get('symbol')} (poll {poll_count})")
                                            await self.callback(sig)
                                            found_dex += 1
                    except Exception as de:
                        logger.debug(f"DexScreener robinhood error: {de}")
                # Heartbeat logging
                if found_logs==0 and found_dex==0 and time.time() - heartbeat > 30:
                    logger.info(f"Robinhood heartbeat: poll {poll_count} no new pairs (GoldRush logs {found_logs} Dex {found_dex}) block {last_block} seen {len(self.seen_tokens)}")
                    heartbeat = time.time()
                await asyncio.sleep(10)
            except Exception as e:
                logger.debug(f"Robinhood GoldRush poll error: {e}")
                if time.time() - heartbeat > 30:
                    logger.info(f"Robinhood heartbeat error poll {poll_count}: {e}")
                    heartbeat = time.time()
                await asyncio.sleep(10)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def stop(self):
        self.running = False
    
    async def _monitor_solana_pumpfun(self, chain_config: dict):
        """Monitor Pump.fun for new token launches via WebSocket + DexScreener fallback"""
        try:
            import websockets
            import base58
            from solders.pubkey import Pubkey
        except ImportError:
            logger.warning("Missing solana dependencies. Install: pip install websockets solders base58")
            return
        
        pump_program = chain_config.get('pump_fun_program', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')

        # WS endpoint chain: a rate-limited or exhausted primary key must never
        # leave the new-pair scout blind. Order: configured primary, configured
        # fallbacks, then verified public endpoints (2026-11-16: Helius free WS
        # returns HTTP 429; publicnode/mainnet accept logsSubscribe).
        ws_candidates = []
        for url in [chain_config.get('ws_url'), *chain_config.get('ws_fallback_urls', [])]:
            if url and '${' not in url and url not in ws_candidates:
                ws_candidates.append(url)
        for url in ('wss://solana-rpc.publicnode.com', 'wss://api.mainnet-beta.solana.com'):
            if url not in ws_candidates:
                ws_candidates.append(url)

        def _ws_label(url: str) -> str:
            return url.split('?', 1)[0]
        
        # DexScreener fallback: solana token-boosts latest
        async def _check_dexscreener_solana():
            import aiohttp
            try:
                async with aiohttp.ClientSession() as s:
                    url2 = "https://api.dexscreener.com/token-boosts/latest/v1"
                    async with s.get(url2, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status == 200:
                            data = await r.json()
                            for t in data[:20]:
                                if t.get('chainId') == 'solana':
                                    token = t.get('tokenAddress','')
                                    if token and token.lower() not in self.seen_tokens:
                                        try:
                                            async with s.get(f"https://api.dexscreener.com/tokens/v1/solana/{token}", timeout=aiohttp.ClientTimeout(total=5)) as pr:
                                                if pr.status == 200:
                                                    pd = await pr.json()
                                                    if isinstance(pd, list) and pd:
                                                        created = pd[0].get('pairCreatedAt', 0)
                                                        age_mins = (datetime.now().timestamp()*1000 - created)/60000 if created else 9999
                                                        if age_mins > 10:
                                                            continue
                                        except: 
                                            age_hours = 0
                                            age_mins = 0
                                        self.seen_tokens.add(token.lower())
                                        sig = {
                                            'token_address': token,
                                            'chain': 'solana',
                                            'source': 'dexscreener_solana',
                                            'score': 65,
                                            'price': 0.0001,
                                            'liquidity_usd': 0,
                                            'detected_at': datetime.now().isoformat(),
                                            'pair_address': t.get('pairAddress',''),
                                            'token_info': {'symbol': t.get('symbol','???'), 'name': t.get('name','Unknown')}
                                        }
                                        logger.info(f"New Solana pair via DexScreener: {token} {t.get('symbol')} (fallback) age {age_mins:.1f}m")
                                        await self.callback(sig)
                                        return True
            except Exception as e:
                logger.debug(f"DexScreener solana fallback error: {e}")
            return False
        
        backoff_step = 0
        while self.running:
            last_error = None
            for endpoint in ws_candidates:
                if not self.running:
                    break
                try:
                    async with websockets.connect(endpoint, open_timeout=20,
                                                  ping_interval=25, ping_timeout=25) as ws:
                        # Subscribe to Pump.fun program logs
                        subscribe_msg = {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "logsSubscribe",
                            "params": [
                                {"mentions": [pump_program]},
                                {"commitment": "processed"}
                            ]
                        }
                        await ws.send(json.dumps(subscribe_msg))
                        logger.info(f"Connected to Solana WebSocket ({_ws_label(endpoint)}), monitoring Pump.fun...")
                        backoff_step = 0
                        last_error = None

                        async for message in ws:
                            if not self.running:
                                break
                            try:
                                data = json.loads(message)
                            except json.JSONDecodeError:
                                continue
                            if data.get('method') == 'logsNotification':
                                await self._handle_pumpfun_logs(data, chain_config)
                        # Stream ended cleanly - fall through and reconnect.
                        break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    last_error = e
                    continue

            if not self.running:
                break

            # All endpoints failed or the live stream dropped: keep the fallback
            # scout working and back off instead of hammering the RPC every 5s.
            await _check_dexscreener_solana()
            if last_error is not None:
                level = logging.WARNING if backoff_step == 0 else logging.DEBUG
                logger.log(level, f"Solana WS unavailable on {len(ws_candidates)} endpoint(s): {last_error}. "
                                  f"DexScreener fallback active.")
            delay = min(60, 5 * (2 ** min(backoff_step, 4)))
            backoff_step += 1
            await asyncio.sleep(delay)
    
    async def _handle_pumpfun_logs(self, data: dict, chain_config: dict):
        """Parse Pump.fun logs for new token creation events.
        Captures the REAL creator (fee payer of the create tx) via one getTransaction call."""
        try:
            result = data.get('params', {}).get('result', {})
            logs = result.get('value', {}).get('logs', [])
            signature = result.get('value', {}).get('signature', '')
            
            # Look for token creation signature
            create_signature = "Program log: create"
            if not any(create_signature in log.lower() for log in logs):
                return
            
            # Extract token address from logs
            for log in logs:
                if "mint:" in log.lower() or "token:" in log.lower():
                    token_address = log.split(":")[-1].strip()
                    if token_address and len(token_address) >= 32 and token_address not in self.seen_tokens:
                        self.seen_tokens.add(token_address)
                        
                        creator = None
                        if signature:
                            try:
                                creator = await self._fetch_creator(signature, chain_config)
                            except Exception as ce:
                                logger.debug(f"Creator fetch failed for {token_address[:8]}: {ce}")
                        
                        signal = {
                            'token_address': token_address,
                            'chain': 'solana',
                            'source': 'pump_fun',
                            'detected_at': datetime.now().isoformat(),
                            'creator': creator,
                            'token_info': await self._fetch_solana_token_info(token_address, chain_config)
                        }
                        
                        if self._passes_filters(signal):
                            logger.info(f"New Pump.fun token detected: {token_address} creator {creator or '??'}")
                            await self.callback(signal)
                        break
                        
        except Exception as e:
            logger.error(f"Error handling Pump.fun logs: {e}")
    
    async def _fetch_creator(self, signature: str, chain_config: dict):
        """Real pump.fun creator = fee payer (first signer) of the create transaction."""
        try:
            import aiohttp
            rpc_url = chain_config.get('rpc_url', 'https://api.mainnet-beta.solana.com')
            payload = {
                "jsonrpc": "2.0", "id": 1, "method": "getTransaction",
                "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
            }
            async with aiohttp.ClientSession() as s:
                async with s.post(rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200:
                        return None
                    data = await r.json()
                    msg = data.get('result', {}).get('transaction', {}).get('message', {})
                    account_keys = msg.get('accountKeys', [])
                    if account_keys:
                        pk = account_keys[0]
                        if isinstance(pk, dict):
                            return (pk.get('pubkey') or '').strip() or None
                        return str(pk).strip() or None
        except Exception:
            return None
        return None
    
    async def _fetch_solana_token_info(self, token_address: str, chain_config: dict) -> dict:
        """Fetch token metadata from Solana RPC"""
        try:
            import aiohttp
            
            rpc_url = chain_config.get('rpc_url', 'https://api.mainnet-beta.solana.com')
            
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getAccountInfo",
                "params": [token_address, {"encoding": "jsonParsed"}]
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(rpc_url, json=payload) as resp:
                    data = await resp.json()
                    account_data = data.get('result', {}).get('value', {}).get('data', {})
                    parsed = account_data.get('parsed', {})
                    
                    return {
                        'name': parsed.get('info', {}).get('name', 'Unknown'),
                        'symbol': parsed.get('info', {}).get('symbol', '???'),
                        'decimals': parsed.get('info', {}).get('decimals', 9),
                        'supply': parsed.get('info', {}).get('supply', 0),
                        'mint_authority': parsed.get('info', {}).get('mintAuthority'),
                        'freeze_authority': parsed.get('info', {}).get('freezeAuthority')
                    }
        except Exception as e:
            logger.debug(f"Could not fetch token info for {token_address}: {e}")
            return {'name': 'Unknown', 'symbol': '???'}
    
    async def _monitor_uniswap_v3(self, chain_config: dict, chain_name: str):
        """Monitor Uniswap V3 factory for new pair creation events"""
        try:
            from web3 import Web3
            import aiohttp
            
            rpc_url = chain_config.get('rpc_url')
            factory_address = chain_config.get('uniswap_v3_factory', '0x1F98431c8aD98523631AE4a59f267346ea31F984')
            
            # Uniswap V3 Factory PairCreated event signature
            pair_created_topic = "0x783cca1c0412dd0d695e71d7c3c031203101cd3024dc4ed9507039e4d38d5f8d"
            
            w3 = Web3(Web3.HTTPProvider(rpc_url))
            
            if not w3.is_connected():
                logger.error(f"Cannot connect to {chain_name} RPC: {rpc_url}")
                return
            
            logger.info(f"Monitoring Uniswap V3 on {chain_name} for new pairs via {rpc_url[:40]}... (live Ankr GoldRush)" if chain_name=='base' else f"Monitoring Uniswap V3 on {chain_name} for new pairs...")
            last_block = w3.eth.block_number
            heartbeat = time.time()
            
            while self.running:
                try:
                    current_block = w3.eth.block_number
                    if current_block <= last_block:
                        # Heartbeat every 30s so backend shows it's alive (user asked to see scanning)
                        if time.time() - heartbeat > 30:
                            logger.info(f"Base scanning heartbeat: block {current_block} seen {len(self.seen_tokens)} tokens (live)")
                            heartbeat = time.time()
                        await asyncio.sleep(2)
                        continue
                    # Heartbeat on new blocks too
                    if time.time() - heartbeat > 30:
                        logger.info(f"Base scanning: blocks {last_block+1}->{current_block} no qualifying new pairs (live)")
                        heartbeat = time.time()
                    
                    # Chunk large ranges (Ankr limit) - max 100 blocks per request
                    chunk_start = last_block + 1
                    while chunk_start <= current_block:
                        chunk_end = min(chunk_start + 99, current_block)
                        try:
                            logs = w3.eth.get_logs({
                                'fromBlock': chunk_start,
                                'toBlock': chunk_end,
                                'address': Web3.to_checksum_address(factory_address),
                                'topics': [pair_created_topic]
                            })
                            for log_entry in logs:
                                await self._handle_uniswap_pair_created(log_entry, chain_name, chain_config)
                        except Exception as ce:
                            if "too large" in str(ce).lower():
                                # Fallback: skip to tip -100 to avoid history scan
                                logger.warning(f"Base range {chunk_start}->{chunk_end} too large, jumping to tip-100")
                                last_block = max(current_block - 100, last_block)
                                break
                            raise
                        chunk_start = chunk_end + 1
                    last_block = current_block
                    
                except Exception as e:
                    logger.error(f"Error scanning {chain_name} blocks: {e}")
                    await asyncio.sleep(5)
                    
        except ImportError:
            logger.warning("Missing web3 dependency. Install: pip install web3")
        except Exception as e:
            logger.error(f"Uniswap V3 monitor error for {chain_name}: {e}")
    
    async def _handle_uniswap_pair_created(self, log_entry: dict, chain_name: str, chain_config: dict):
        """Handle new Uniswap V3 pair creation"""
        try:
            from web3 import Web3
            
            topics = log_entry.get('topics', [])
            if len(topics) < 3:
                return
            
            token0 = '0x' + topics[1][-40:]
            token1 = '0x' + topics[2][-40:]
            
            # Check if it's a WETH pair (common for new tokens)
            weth_addresses = {
                'base': '0x4200000000000000000000000000000000000006',
                'robinhood': '0x4200000000000000000000000000000000000006'
            }
            weth = weth_addresses.get(chain_name, '').lower()
            
            new_token = None
            if token0.lower() == weth:
                new_token = token1
            elif token1.lower() == weth:
                new_token = token0
            
            if new_token and new_token.lower() not in self.seen_tokens:
                self.seen_tokens.add(new_token.lower())
                
                signal = {
                    'token_address': new_token,
                    'chain': chain_name,
                    'source': 'uniswap_v3',
                    'detected_at': datetime.now().isoformat(),
                    'pair_address': log_entry.get('address', ''),
                    'token_info': await self._fetch_erc20_info(new_token, chain_config)
                }
                
                if self._passes_filters(signal):
                    logger.info(f"New Uniswap pair on {chain_name}: {new_token}")
                    await self.callback(signal)
                    
        except Exception as e:
            logger.error(f"Error handling Uniswap pair: {e}")
    
    async def _fetch_erc20_info(self, token_address: str, chain_config: dict) -> dict:
        """Fetch ERC-20 token metadata"""
        try:
            from web3 import Web3
            
            w3 = Web3(Web3.HTTPProvider(chain_config['rpc_url']))
            
            # Standard ERC-20 ABI for name, symbol, decimals
            erc20_abi = [
                {"constant": True, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}], "type": "function"},
                {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
                {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"},
            ]
            
            contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=erc20_abi)
            
            name = contract.functions.name().call()
            symbol = contract.functions.symbol().call()
            decimals = contract.functions.decimals().call()
            
            return {'name': name, 'symbol': symbol, 'decimals': decimals}
            
        except Exception as e:
            logger.debug(f"Could not fetch ERC-20 info for {token_address}: {e}")
            return {'name': 'Unknown', 'symbol': '???', 'decimals': 18}
    
    def _passes_filters(self, signal: dict) -> bool:
        """Check if detected token passes basic filters"""
        token_address = signal.get('token_address', '').lower()
        
        # Skip blacklisted tokens
        blacklisted = self.config.get('blacklisted_tokens', [])
        if token_address in [t.lower() for t in blacklisted]:
            return False
        
        return True

async def _monitor_robinhood_goldrush(self, chain_config: dict):
        """Phase1 Scanner: GoldRush minute-level PairCreated (robinhood 4663) via logs + DexScreener fallback. Heartbeat 30s."""
        import aiohttp
        logger.info("Monitoring Robinhood Chain via GoldRush logs PairCreated 0x783cca1... (robinhood-mainnet 4663) for new pairs every minute...")
        heartbeat = time.time()
        poll_count = 0
        last_block = 0
        try:
            async with aiohttp.ClientSession() as s:
                rpc_url = "https://rpc.goldrushdata.com/v1/robinhood-mainnet"
                payload = {"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}
                goldrush_key = self.config.get('goldrush',{}).get('api_key','')
                async with s.post(rpc_url, json=payload, headers={"Authorization": f"Bearer {goldrush_key}"}, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status == 200:
                        j = await r.json()
                        last_block = int(j.get('result','0x0'), 16) if j.get('result') else 0
                        logger.info(f"Robinhood tip block {last_block} (GoldRush RPC)")
        except: pass
        if last_block == 0:
            last_block = 48540000

        while self.running:
            try:
                poll_count += 1
                # --- 1. Force advance block each poll + try GoldRush logs by topic ---
                found_logs = 0
                cur_block = last_block + 20  # always advance forward 20 blocks
                logs = []
                api = None
                # Get clean key (strip ${} template if present)
                goldrush_key = self.config.get('goldrush',{}).get('api_key','')
                actual_key = goldrush_key
                if goldrush_key and goldrush_key.startswith('${') and goldrush_key.endswith('}'):
                    actual_key = goldrush_key[2:-1]
                if actual_key:
                    try:
                        from utils.goldrush_api import GoldRushAPI
                        api = GoldRushAPI(actual_key)
                        # Scan the forced-advance range for PairCreated logs
                        chunk_start = last_block + 1
                        chunk_end = min(chunk_start + 99, cur_block)
                        if chunk_start <= chunk_end:
                            logs = await api.get_logs_by_topic("robinhood-mainnet", "0x783cca1c0412dd0d695e71d7c3c031203101cd3024dc4ed9507039e4d38d5f8d", chunk_start, chunk_end)
                    except Exception as ge:
                        logger.debug(f"GoldRush logs error: {ge}")
                        logs = []
                # Parse each log for token addresses
                found_logs = 0
                if logs:
                    for log in logs:
                        try:
                            topics = log.get('raw_log_topics', []) or log.get('topics', [])
                            if len(topics) >= 3:
                                t0 = '0x' + topics[1][-40:] if isinstance(topics[1], str) else ''
                                t1 = '0x' + topics[2][-40:] if isinstance(topics[2], str) else ''
                                weth_like = '0x117cc2133c37B721F49dE2A7a74833232B3B4C0C'.lower()
                                new_tok = None
                                if t0.lower() != weth_like and t0.lower() != '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee':
                                    new_tok = t0
                                elif t1.lower() != weth_like:
                                    new_tok = t1
                                if new_tok and new_tok.lower() not in self.seen_tokens:
                                    self.seen_tokens.add(new_tok.lower())
                                    sig = {
                                        'token_address': new_tok,
                                        'chain': 'robinhood',
                                        'source': 'goldrush_logs',
                                        'score': 70,
                                        'price': 0.0001,
                                        'liquidity_usd': 0,
                                        'detected_at': datetime.now().isoformat(),
                                        'pair_address': log.get('address','')
                                    }
                                    logger.info(f"New Robinhood pair via GoldRush logs: {new_tok} block {cur_block} (poll {poll_count})")
                                    await self.callback(sig)
                                    found_logs += 1
                        except Exception as e:
                            logger.debug(f"GoldRush log parse error: {e}")
                # --- 2. DexScreener token-boosts fallback (catch any new token, no age filter) ---
                found_dex = 0
                async with aiohttp.ClientSession() as s:
                    try:
                        url2 = "https://api.dexscreener.com/token-boosts/latest/v1"
                        async with s.get(url2, timeout=aiohttp.ClientTimeout(total=10)) as r:
                            if r.status == 200:
                                data = await r.json()
                                for t in data[:20]:
                                    if t.get('chainId') == 'robinhood':
                                        token = t.get('tokenAddress','')
                                        if token and token.lower() not in self.seen_tokens:
                                            try:
                                                async with s.get(f"https://api.dexscreener.com/tokens/v1/robinhood/{token}", timeout=aiohttp.ClientTimeout(total=5)) as pr:
                                                    if pr.status == 200:
                                                        pd = await pr.json()
                                                        if isinstance(pd, list) and pd:
                                                            pass  # any pairCreatedAt is fine - catch all
                                            except: pass
                                            self.seen_tokens.add(token.lower())
                                            sig = {
                                                'token_address': token,
                                                'chain': 'robinhood',
                                                'source': 'dexscreener_robinhood',
                                                'score': 65,
                                                'price': 0.0001,
                                                'liquidity_usd': 0,
                                                'detected_at': datetime.now().isoformat(),
                                                'pair_address': t.get('pairAddress',''),
                                                'token_info': {'symbol': t.get('symbol','???'), 'name': t.get('name','Unknown')}
                                            }
                                            logger.info(f"New Robinhood pair via DexScreener: {token} {t.get('symbol')} (poll {poll_count})")
                                            await self.callback(sig)
                                            found_dex += 1
                    except Exception as de:
                        logger.debug(f"DexScreener robinhood error: {de}")
                # Heartbeat logging
                if found_logs==0 and found_dex==0 and time.time() - heartbeat > 30:
                    logger.info(f"Robinhood heartbeat: poll {poll_count} no new pairs (GoldRush logs {found_logs} Dex {found_dex}) block {last_block} seen {len(self.seen_tokens)}")
                    heartbeat = time.time()
                await asyncio.sleep(10)
            except Exception as e:
                logger.debug(f"Robinhood GoldRush poll error: {e}")
                if time.time() - heartbeat > 30:
                    logger.info(f"Robinhood heartbeat error poll {poll_count}: {e}")
                    heartbeat = time.time()
                await asyncio.sleep(10)
            except Exception as e:
                logger.debug(f"Robinhood GoldRush poll error: {e}")
                if time.time() - heartbeat > 30:
                    logger.info(f"Robinhood heartbeat error poll {poll_count}: {e}")
                    heartbeat = time.time()
                await asyncio.sleep(10)

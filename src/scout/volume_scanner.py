import asyncio
import logging
import time
from typing import Callable, Dict, List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class VolumeScanner:
    """Detects tokens with sudden volume spikes"""
    
    def __init__(self, config: dict, callback: Callable):
        self.config = config.get('scout', {}).get('volume_scan', {})
        self.callback = callback
        self.running = False
        self.volume_history: Dict[str, List[dict]] = {}
        self.spike_threshold = self.config.get('volume_spike_threshold', 5)
        self.min_volume = self.config.get('min_volume_usd', 10000)
        self.scan_interval = self.config.get('scan_interval_seconds', 30)
        self.lookback_minutes = self.config.get('lookback_minutes', 60)
        self.alerted_tokens = set()
    
    async def start(self, chains_config: dict):
        """Start volume scanning across chains"""
        self.running = True
        logger.info(f"Volume scanner started (threshold: {self.spike_threshold}x, interval: {self.scan_interval}s)")
        
        while self.running:
            try:
                await self._scan_all_chains(chains_config)
                await asyncio.sleep(self.scan_interval)
            except Exception as e:
                logger.error(f"Volume scan error: {e}")
                await asyncio.sleep(5)
    
    async def stop(self):
        self.running = False
    
    async def _scan_all_chains(self, chains_config: dict):
        """Scan volume data for all enabled chains"""
        tasks = []
        
        for chain_name, chain_config in chains_config.items():
            if chain_config.get('enabled'):
                tasks.append(self._scan_chain_volume(chain_name, chain_config))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _scan_chain_volume(self, chain_name: str, chain_config: dict):
        """Scan volume for a specific chain using DexScreener API"""
        try:
            import aiohttp
            
            # DexScreener API - get trending tokens
            url = f"https://api.dexscreener.com/token-boosts/latest/v1"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        return
                    
                    data = await resp.json()
                    
                    for token in data:
                        chain = token.get('chainId', '')
                        
                        # Map chain names
                        chain_map = {'solana': 'solana', 'base': 'base', 'ethereum': 'ethereum'}
                        if chain not in chain_map:
                            continue
                        
                        token_address = token.get('tokenAddress', '')
                        pair_address = token.get('pairAddress', '')
                        
                        if not token_address:
                            continue
                        
                        # Fetch detailed pair data
                        pair_data = await self._fetch_pair_data(session, pair_address)
                        if not pair_data:
                            continue
                        
                        await self._analyze_volume(token_address, chain, pair_data)
            
            # Also check Birdeye if configured
            birdeye_key = chain_config.get('birdeye_api_key')
            if birdeye_key:
                await self._scan_birdeye_volume(chain_name, chain_config, birdeye_key)
                
        except Exception as e:
            logger.debug(f"Volume scan error for {chain_name}: {e}")
    
    async def _fetch_pair_data(self, session, pair_address: str) -> Optional[dict]:
        """Fetch detailed pair data from DexScreener"""
        try:
            url = f"https://api.dexscreener.com/latest/dex/pairs/{pair_address}"
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get('pairs', [])
                    return pairs[0] if pairs else None
        except Exception as e:
            logger.debug(f"Error fetching pair data: {e}")
        return None
    
    async def _analyze_volume(self, token_address: str, chain: str, pair_data: dict):
        """Analyze volume data for spikes"""
        try:
            volume_24h = pair_data.get('volume', {}).get('h24', 0)
            volume_6h = pair_data.get('volume', {}).get('h6', 0)
            volume_1h = pair_data.get('volume', {}).get('h1', 0)
            volume_5m = pair_data.get('volume', {}).get('m5', 0)
            
            price_change_24h = pair_data.get('priceChange', {}).get('h24', 0)
            price_change_1h = pair_data.get('priceChange', {}).get('h1', 0)
            
            liquidity_usd = pair_data.get('liquidity', {}).get('usd', 0)
            
            key = f"{chain}:{token_address}"
            
            # Store volume history
            if key not in self.volume_history:
                self.volume_history[key] = []
            
            self.volume_history[key].append({
                'timestamp': time.time(),
                'volume_1h': volume_1h,
                'volume_5m': volume_5m
            })
            
            # Keep only recent history
            cutoff = time.time() - (self.lookback_minutes * 60)
            self.volume_history[key] = [
                v for v in self.volume_history[key] if v['timestamp'] > cutoff
            ]
            
            # Calculate volume spike
            history = self.volume_history[key]
            if len(history) < 2:
                return
            
            # Compare current 1h volume to average
            avg_volume = sum(v['volume_1h'] for v in history[:-1]) / max(len(history) - 1, 1)
            current_volume = volume_1h
            
            if avg_volume > 0:
                volume_ratio = current_volume / avg_volume
            else:
                volume_ratio = 999 if current_volume > self.min_volume else 0
            
            # Check for spike
            if (volume_ratio >= self.spike_threshold and 
                current_volume >= self.min_volume and
                key not in self.alerted_tokens):
                
                self.alerted_tokens.add(key)
                
                signal = {
                    'token_address': token_address,
                    'chain': chain,
                    'source': 'volume_scanner',
                    'detected_at': datetime.now().isoformat(),
                    'volume_ratio': round(volume_ratio, 2),
                    'volume_1h': volume_1h,
                    'volume_24h': volume_24h,
                    'price_change_1h': price_change_1h,
                    'price_change_24h': price_change_24h,
                    'liquidity_usd': liquidity_usd,
                    'pair_address': pair_data.get('pairAddress', ''),
                    'base_token': pair_data.get('baseToken', {}).get('name', ''),
                    'score': self._calculate_score(volume_ratio, volume_1h, liquidity_usd, price_change_1h)
                }
                
                logger.info(f"Volume spike detected: {token_address} on {chain} ({volume_ratio:.1f}x)")
                await self.callback(signal)
                
                # Remove from alerted after cooldown
                asyncio.create_task(self._cooldown(key, 300))
                
        except Exception as e:
            logger.debug(f"Error analyzing volume for {token_address}: {e}")
    
    def _calculate_score(self, volume_ratio: float, volume_1h: float, 
                         liquidity: float, price_change_1h: float) -> float:
        """Calculate a score for the volume signal (0-100)"""
        score = 0
        
        # Volume spike magnitude (0-40 points)
        if volume_ratio >= 20:
            score += 40
        elif volume_ratio >= 10:
            score += 30
        elif volume_ratio >= 5:
            score += 20
        else:
            score += 10
        
        # Volume size (0-30 points)
        if volume_1h >= 100000:
            score += 30
        elif volume_1h >= 50000:
            score += 20
        elif volume_1h >= 10000:
            score += 10
        
        # Liquidity (0-20 points)
        if liquidity >= 50000:
            score += 20
        elif liquidity >= 20000:
            score += 15
        elif liquidity >= 5000:
            score += 10
        
        # Price momentum (0-10 points)
        if price_change_1h > 50:
            score += 10
        elif price_change_1h > 20:
            score += 5
        
        return min(score, 100)
    
    async def _cooldown(self, key: str, seconds: int):
        """Remove token from alerted set after cooldown"""
        await asyncio.sleep(seconds)
        self.alerted_tokens.discard(key)
    
    async def _scan_birdeye_volume(self, chain_name: str, chain_config: dict, api_key: str):
        """Scan volume using Birdeye API"""
        try:
            import aiohttp
            
            birdeye_chain = 'solana' if chain_name == 'solana' else 'base'
            
            url = f"https://public-api.birdeye.so/defi/token_trending"
            headers = {
                'X-API-KEY': api_key,
                'x-chain': birdeye_chain
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        return
                    
                    data = await resp.json()
                    tokens = data.get('data', {}).get('tokens', [])
                    
                    for token in tokens:
                        token_address = token.get('address', '')
                        volume_24h = token.get('volume24hUSD', 0)
                        unique_wallets = token.get('uniqueWallet24h', 0)
                        
                        if volume_24h >= self.min_volume and unique_wallets >= 10:
                            # Check if this is a new token (high unique wallets relative to age)
                            signal = {
                                'token_address': token_address,
                                'chain': chain_name,
                                'source': 'birdeye_volume',
                                'detected_at': datetime.now().isoformat(),
                                'volume_24h': volume_24h,
                                'unique_wallets': unique_wallets,
                                'price': token.get('price', 0),
                                'score': min(50 + (unique_wallets // 10), 100)
                            }
                            
                            key = f"{chain_name}:{token_address}"
                            if key not in self.alerted_tokens:
                                self.alerted_tokens.add(key)
                                await self.callback(signal)
                                asyncio.create_task(self._cooldown(key, 300))
                                
        except Exception as e:
            logger.debug(f"Birdeye volume scan error: {e}")

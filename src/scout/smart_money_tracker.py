import asyncio
import logging
import time
import os
from typing import Callable, Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

try:
    from ..utils.ankr_api import AnkrAPI
except ImportError:
    try:
        from utils.ankr_api import AnkrAPI
    except ImportError:
        AnkrAPI = None

class SmartMoneyTracker:
    """Tracks known profitable wallets for early token buys"""
    
    def __init__(self, config: dict, callback: Callable):
        self.config = config.get('scout', {}).get('smart_money', {})
        self.callback = callback
        self.running = False
        self.tracked_wallets = set(self.config.get('tracked_wallets', []))
        self.scan_interval = self.config.get('scan_interval_seconds', 10)
        self.min_trade_size = self.config.get('min_trade_size_usd', 500)
        self.wallet_buy_history: Dict[str, Dict[str, float]] = {}
        self.alerted_buys = set()
        # Init Ankr API if key available (large free tier: 50 req/min multichain)
        ankr_key = config.get('ankr', {}).get('api_key') or os.getenv('ANKR_API_KEY')
        if ankr_key and AnkrAPI and not ankr_key.startswith('${'):
            self.ankr = AnkrAPI(ankr_key)
            logger.info("Ankr API enabled for smart money tracking (multichain)")
        else:
            self.ankr = None
            if not ankr_key or ankr_key.startswith('${'):
                logger.info("Ankr API key not set - using direct RPC fallback")
    
    async def start(self, chains_config: dict):
        """Start tracking smart money wallets"""
        self.running = True
        
        if not self.tracked_wallets:
            logger.warning("No wallets configured for smart money tracking")
            return
        
        logger.info(f"Smart money tracker started. Tracking {len(self.tracked_wallets)} wallets")
        
        while self.running:
            try:
                await self._scan_wallets(chains_config)
                await asyncio.sleep(self.scan_interval)
            except Exception as e:
                logger.error(f"Smart money scan error: {e}")
                await asyncio.sleep(5)
    
    async def stop(self):
        self.running = False
    
    def add_wallet(self, address: str, label: str = ""):
        """Add a wallet to track"""
        self.tracked_wallets.add(address.lower())
        logger.info(f"Added wallet to track: {address} ({label})")
    
    def remove_wallet(self, address: str):
        """Remove a tracked wallet"""
        self.tracked_wallets.discard(address.lower())
    
    async def _scan_wallets(self, chains_config: dict):
        """Scan all tracked wallets for recent buys"""
        tasks = []
        
        for chain_name, chain_config in chains_config.items():
            if chain_config.get('enabled'):
                tasks.append(self._scan_chain_wallets(chain_name, chain_config))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _scan_chain_wallets(self, chain_name: str, chain_config: dict):
        """Scan wallets on a specific chain"""
        try:
            if chain_name == 'solana':
                await self._scan_solana_wallets(chain_config)
            else:
                await self._scan_evm_wallets(chain_name, chain_config)
        except Exception as e:
            logger.debug(f"Error scanning {chain_name} wallets: {e}")
    
    async def _scan_solana_wallets(self, chain_config: dict):
        """Scan Solana wallets using Helius or standard RPC"""
        try:
            import aiohttp
            
            rpc_url = chain_config.get('rpc_url', 'https://api.mainnet-beta.solana.com')
            helius_key = chain_config.get('helius_api_key')
            
            async with aiohttp.ClientSession() as session:
                for wallet in self.tracked_wallets:
                    # Convert EVM format to Solana if needed
                    if wallet.startswith('0x'):
                        continue  # Skip EVM addresses for Solana
                    
                    await self._check_solana_wallet_activity(
                        session, wallet, rpc_url, helius_key
                    )
                    
        except Exception as e:
            logger.debug(f"Solana wallet scan error: {e}")
    
    async def _check_solana_wallet_activity(self, session, wallet: str, rpc_url: str, helius_key: str = None):
        """Check a single Solana wallet for recent activity"""
        try:
            # Use Helius enhanced transactions if available
            if helius_key:
                url = f"https://api.helius.xyz/v0/addresses/{wallet}/transactions?api-key={helius_key}&limit=5"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for tx in data:
                            await self._process_solana_transaction(wallet, tx)
            else:
                # Standard RPC
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getSignaturesForAddress",
                    "params": [wallet, {"limit": 5}]
                }
                async with session.post(rpc_url, json=payload) as resp:
                    data = await resp.json()
                    signatures = data.get('result', [])
                    # Would need to fetch each tx for full details
                    # Simplified for now
                    
        except Exception as e:
            logger.debug(f"Error checking Solana wallet {wallet}: {e}")
    
    async def _process_solana_transaction(self, wallet: str, tx: dict):
        """Process a Solana transaction from Helius"""
        try:
            # Check if it's a token swap/buy
            token_transfers = tx.get('tokenTransfers', [])
            native_transfers = tx.get('nativeTransfers', [])
            
            # Look for SOL going out (buy) and tokens coming in
            sol_out = 0
            tokens_in = []
            
            for transfer in native_transfers:
                if transfer.get('toUserAccount', '').lower() != wallet.lower():
                    sol_out += transfer.get('amount', 0) / 1e9  # lamports to SOL
            
            for transfer in token_transfers:
                if transfer.get('toUserAccount', '').lower() == wallet.lower():
                    tokens_in.append({
                        'mint': transfer.get('mint', ''),
                        'amount': transfer.get('tokenAmount', 0),
                        'symbol': transfer.get('tokenStandard', 'unknown')
                    })
            
            # If SOL went out and tokens came in, it's a buy
            if sol_out > 0 and tokens_in:
                sol_usd = sol_out * 300  # Rough SOL price estimate
                
                if sol_usd >= self.min_trade_size:
                    for token in tokens_in:
                        buy_key = f"{wallet}:{token['mint']}:{tx.get('signature', '')}"
                        
                        if buy_key not in self.alerted_buys:
                            self.alerted_buys.add(buy_key)
                            
                            signal = {
                                'token_address': token['mint'],
                                'chain': 'solana',
                                'source': 'smart_money',
                                'detected_at': datetime.now().isoformat(),
                                'smart_wallet': wallet,
                                'trade_size_usd': sol_usd,
                                'token_amount': token['amount'],
                                'tx_hash': tx.get('signature', ''),
                                'score': self._calculate_wallet_score(wallet, sol_usd)
                            }
                            
                            logger.info(f"Smart money buy detected: {wallet[:8]}... bought {token['mint'][:8]}... (${sol_usd:.0f})")
                            await self.callback(signal)
                            
                            # Clean old alerts periodically
                            if len(self.alerted_buys) > 1000:
                                self.alerted_buys = set(list(self.alerted_buys)[-500:])
                            
        except Exception as e:
            logger.debug(f"Error processing Solana tx: {e}")
    
    async def _scan_evm_wallets(self, chain_name: str, chain_config: dict):
        """Scan EVM wallets (Base, Robinhood Chain) - Ankr first, fallback to direct RPC"""
        if self.ankr:
            # Skip Ankr for robinhood - not supported, use GoldRush fallback
            if chain_name == 'robinhood':
                logger.debug(f"Skipping Ankr for robinhood {chain_name}, using GoldRush fallback")
            else:
                try:
                    ankr_chain = 'base' if chain_name == 'base' else 'eth' if chain_name == 'ethereum' else 'arbitrum'
                    for wallet in self.tracked_wallets:
                        if not wallet.startswith('0x'):
                            continue
                        transfers = await self.ankr.get_token_transfers(wallet, ankr_chain, page_size=10)
                        for tf in transfers:
                            if tf.get('to', '').lower() != wallet.lower():
                                continue
                            token_address = tf.get('token_address', '')
                            if not token_address:
                                continue
                            buy_key = f"{wallet}:{token_address}:{tf.get('hash','')}"
                            if buy_key in self.alerted_buys:
                                continue
                            try:
                                raw_val = tf.get('value', '0')
                                decimals = tf.get('token_decimals', 18)
                                amount_float = float(raw_val) / (10 ** decimals) if raw_val else 0
                            except:
                                amount_float = 0
                            if amount_float == 0:
                                continue
                            self.alerted_buys.add(buy_key)
                            signal = {
                                'token_address': token_address,
                                'chain': chain_name,
                                'source': 'smart_money_ankr',
                                'detected_at': datetime.now().isoformat(),
                                'smart_wallet': wallet,
                                'amount_raw': tf.get('value'),
                                'token_symbol': tf.get('token_symbol'),
                                'tx_hash': tf.get('hash',''),
                                'score': self._calculate_wallet_score(wallet, amount_float)
                            }
                            logger.info(f"Smart money [Ankr] buy: {wallet[:8]}... -> {token_address[:8]}... ({tf.get('token_symbol','???')}) on {chain_name}")
                            await self.callback(signal)
                    return
                except Exception as e:
                    logger.debug(f"Ankr smart money scan failed, falling back to RPC: {e}")

        # Fallback: direct RPC getLogs
        try:
            import aiohttp
            from web3 import Web3
            
            rpc_url = chain_config.get('rpc_url')
            w3 = Web3(Web3.HTTPProvider(rpc_url))
            
            if not w3.is_connected():
                return
            
            async with aiohttp.ClientSession() as session:
                for wallet in self.tracked_wallets:
                    if not wallet.startswith('0x'):
                        continue
                    
                    await self._check_evm_wallet_activity(
                        session, w3, wallet, chain_name, chain_config
                    )
                    
        except ImportError:
            logger.warning("Missing web3 dependency")
        except Exception as e:
            logger.debug(f"EVM wallet scan error: {e}")
    
    async def _check_evm_wallet_activity(self, session, w3, wallet: str, chain_name: str, chain_config: dict):
        """Check EVM wallet for recent token buys"""
        try:
            checksum_wallet = Web3.to_checksum_address(wallet)
            current_block = w3.eth.block_number
            from_block = current_block - 100  # Last ~20 minutes
            
            # ERC-20 Transfer event signature
            transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            
            # Get recent transfers TO this wallet
            logs = w3.eth.get_logs({
                'fromBlock': from_block,
                'toBlock': current_block,
                'topics': [
                    transfer_topic,
                    None,
                    '0x' + '0' * 24 + checksum_wallet[2:].lower()
                ]
            })
            
            for log_entry in logs:
                token_address = log_entry.get('address', '')
                topics = log_entry.get('topics', [])
                
                if len(topics) >= 4:
                    amount_hex = topics[3].hex() if isinstance(topics[3], bytes) else topics[3]
                    amount = int(amount_hex, 16)
                    
                    if amount > 0:
                        buy_key = f"{wallet}:{token_address}:{log_entry.get('transactionHash', '').hex()}"
                        
                        if buy_key not in self.alerted_buys:
                            self.alerted_buys.add(buy_key)
                            
                            signal = {
                                'token_address': token_address,
                                'chain': chain_name,
                                'source': 'smart_money',
                                'detected_at': datetime.now().isoformat(),
                                'smart_wallet': wallet,
                                'amount_raw': amount,
                                'tx_hash': log_entry.get('transactionHash', '').hex(),
                                'block': log_entry.get('blockNumber', 0),
                                'score': self._calculate_wallet_score(wallet, 0)
                            }
                            
                            logger.info(f"Smart money buy detected: {wallet[:8]}... bought {token_address[:8]}... on {chain_name}")
                            await self.callback(signal)
                            
        except Exception as e:
            logger.debug(f"Error checking EVM wallet {wallet}: {e}")
    
    def _calculate_wallet_score(self, wallet: str, trade_size_usd: float) -> float:
        """Calculate confidence score based on wallet reputation and trade size"""
        score = 50  # Base score for tracked wallets
        
        # Boost for larger trades (more conviction)
        if trade_size_usd >= 5000:
            score += 30
        elif trade_size_usd >= 2000:
            score += 20
        elif trade_size_usd >= 1000:
            score += 10
        elif trade_size_usd >= 500:
            score += 5
        
        # Could add more sophisticated scoring based on:
        # - Historical win rate of this wallet
        # - How early it typically buys
        # - Whether it's a known dev/insider
        
        return min(score, 100)
    
    def load_wallets_from_file(self, filepath: str):
        """Load tracked wallets from a file (one address per line)"""
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        self.tracked_wallets.add(line.lower())
            logger.info(f"Loaded {len(self.tracked_wallets)} wallets from {filepath}")
        except FileNotFoundError:
            logger.warning(f"Wallet file not found: {filepath}")

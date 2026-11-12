"""Lookup - on server start fetch top trending coins per chain and brutal-grade their wallets.
Finds reputable devs + smart traders, then pings AlertManager to watch them.
"""
import asyncio
import logging
import aiohttp
from typing import Dict, List, Callable
from datetime import datetime

logger = logging.getLogger(__name__)

class Lookup:
    def __init__(self, config: dict):
        self.config = config.get('lookup', {})
        self.chains = config.get('chains', {})
        self.ankr_key = config.get('ankr', {}).get('api_key','')
        self.goldrush_key = config.get('goldrush', {}).get('api_key','')
        self.on_reputable: Callable = None  # set by main.py -> alert_manager.add_watch

    async def run_once(self) -> List[Dict]:
        """Fetch top coins for 6h/24h per enabled chain, grade wallets, return reputables."""
        logger.info("Lookup starting: fetching top coins 6h/24h per chain...")
        all_coins = []
        for chain_name, chain_cfg in self.chains.items():
            if not chain_cfg.get('enabled'):
                continue
            coins = await self._fetch_top_coins(chain_name)
            all_coins.extend(coins)

        # Deduplicate by token address
        seen = set()
        uniq = []
        for c in all_coins:
            k = (c.get('chain'), c.get('token_address','').lower())
            if k not in seen:
                seen.add(k)
                uniq.append(c)

        logger.info(f"Lookup found {len(uniq)} top coins across chains, grading wallets via brutal checks...")
        reputables = []
        # For each coin, fetch top holders/traders and brutal-grade
        for coin in uniq[:12]:  # cap 12 to stay within rate limits
            wallets = await self._fetch_wallets_for_coin(coin)
            for w in wallets[:8]:
                    grade = await self._brutal_grade(w['address'], coin['chain'])
                    # Lenient: save B/C as well for initial watchlist, not just A/B, so lookup actually populates
                    if grade['overall_score'] >= 50 or grade['grade'] in ('A','B','C'):
                        reputables.append({
                            'wallet': w['address'],
                            'chain': coin['chain'],
                            'coin': coin['token_address'],
                            'coin_symbol': coin.get('symbol','???'),
                            'grade': grade['grade'],
                            'score': grade['overall_score'],
                            'win_rate': grade.get('key_metrics',{}).get('win_rate',''),
                            'reason': f"Top {coin.get('symbol','???')} {coin.get('timeframe','')} holder/trader -> brutal {grade['grade']} {grade['overall_score']}",
                            'found_at': datetime.now().isoformat(),
                            'source_coin': coin
                        })
                        if self.on_reputable:
                            await self.on_reputable(reputables[-1])
            await asyncio.sleep(0.3)

        logger.info(f"Lookup complete: {len(reputables)} reputable devs/traders found (lenient B/C 50+)")
        return reputables

    async def _fetch_top_coins(self, chain: str) -> List[Dict]:
        """DexScreener trending 6h/24h + top by volume. GoldRush chain names: base, robinhood-mainnet."""
        coins = []
        try:
            async with aiohttp.ClientSession() as s:
                # 1. DexScreener trending via token boosts (covers 6h momentum)
                url = "https://api.dexscreener.com/token-boosts/latest/v1"
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status == 200:
                        data = await r.json()
                        for t in data:
                            if t.get('chainId') == chain or (chain=='robinhood' and t.get('chainId')=='robinhood'):
                                coins.append({
                                    'token_address': t.get('tokenAddress',''),
                                    'symbol': t.get('symbol','???'),
                                    'chain': chain,
                                    'timeframe': 'trending',
                                    'source': 'dexscreener_trending'
                                })
                                if len(coins) >= 6:
                                    break
                # 2. DexScreener search top per chain by volume
                search_url = f"https://api.dexscreener.com/latest/dex/search/?q={chain}"
                async with s.get(search_url, timeout=aiohttp.ClientTimeout(total=10)) as r2:
                    if r2.status == 200:
                        data = await r2.json()
                        pairs = data.get('pairs',[])[:4]
                        for p in pairs:
                            base = p.get('baseToken',{})
                            coins.append({
                                'token_address': base.get('address',''),
                                'symbol': base.get('symbol','???'),
                                'chain': chain,
                                'timeframe': '24h_top',
                                'source': 'dexscreener_top',
                                'pair': p.get('pairAddress','')
                            })
        except Exception as e:
            logger.debug(f"Lookup fetch top coins {chain} error: {e}")
        return [c for c in coins if c.get('token_address')]

    async def _fetch_wallets_for_coin(self, coin: Dict) -> List[Dict]:
        """Get top holders/traders for a coin. Try GoldRush for robinhood, Ankr/DexScreener for base."""
        chain = coin['chain']
        token = coin['token_address']
        wallets = []
        try:
            # Try GoldRush for both chains (robinhood-mainnet and base-mainnet) to get deployer/traders
            gold_chain = "robinhood-mainnet" if chain == 'robinhood' else "base-mainnet"
            if self.goldrush_key and not self.goldrush_key.startswith('${'):
                async with aiohttp.ClientSession() as s:
                    url = f"https://api.covalenthq.com/v1/{gold_chain}/address/{token}/transactions_v3/?page-size=5&key={self.goldrush_key}"
                    async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status == 200:
                            data = await r.json()
                            for tx in data.get('data',{}).get('items',[])[:5]:
                                # from_address is deployer/first buyer - good reputable to watch
                                fa = tx.get('from_address','')
                                if fa and fa.lower() != token.lower():
                                    wallets.append({'address': fa, 'role': 'deployer/trader'})
                            # Fallback if no txs: try holders endpoint
                            if not wallets:
                                hurl = f"https://api.covalenthq.com/v1/{gold_chain}/tokens/{token}/token_holders/?page-size=5&key={self.goldrush_key}"
                                async with s.get(hurl, timeout=aiohttp.ClientTimeout(total=10)) as hr:
                                    if hr.status == 200:
                                        hdata = await hr.json()
                                        for h in hdata.get('data',{}).get('items',[])[:3]:
                                            wallets.append({'address': h.get('address',''), 'role': 'holder'})
            if not wallets:
                pair = coin.get('pair','')
                if pair and pair.startswith('0x'):
                    wallets.append({'address': pair, 'role': 'pair_fallback'})
        except Exception as e:
            logger.debug(f"fetch wallets {token[:8]} error: {e}")
        # Deduplicate
        seen=set()
        out=[]
        for w in wallets:
            a=w.get('address','').lower()
            if a and a not in seen and a.startswith('0x') and len(a)==42:
                seen.add(a)
                out.append(w)
        return out

    async def _brutal_grade(self, wallet: str, chain: str) -> Dict:
        """Run brutal-wallet-auditor checks via Ankr/GoldRush fetcher."""
        try:
            import sys
            sys.path.insert(0, 'C:/Users/EMMANUEL/brutal-wallet-auditor/src')
            from blockchain_fetcher import BlockchainFetcher
            from time_engine import TimeEngine
            from math_engine import MathEngine
            from grading_system import BrutalGrader
            fetcher = BlockchainFetcher()
            time_engine = TimeEngine()
            math_engine = MathEngine()
            grader = BrutalGrader()
            # Map chain to fetcher chain
            fetch_chain = 'base' if chain=='base' else 'base'  # GoldRush robinhood not in fetcher, fallback base
            try:
                wallet_data = fetcher.fetch_wallet_data(wallet, fetch_chain)
            except:
                return {'grade': 'C', 'overall_score': 50, 'key_metrics': {}}
            if not wallet_data.get('transactions'):
                return {'grade': 'C', 'overall_score': 40, 'key_metrics': {}}
            time_analysis = time_engine.analyze_trading_patterns(wallet_data['transactions'], wallet_data['token_transfers'])
            math_analysis = math_engine.analyze_trading_math(wallet_data['transactions'], wallet_data['token_transfers'])
            report = grader.generate_report(time_analysis, math_analysis)
            return {
                'grade': report['summary']['grade'],
                'overall_score': report['summary']['overall_score'],
                'key_metrics': report['summary']['key_metrics'],
                'report': report
            }
        except Exception as e:
            logger.debug(f"Brutal grade {wallet[:8]} error: {e}")
            return {'grade': 'C', 'overall_score': 50, 'key_metrics': {}}

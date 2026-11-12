"""AlertManager - watches reputable devs/traders found by Lookup.
If watched wallet trades a new token, fire alert (log + dashboard + webhook).
Uses GoldRush for robinhood-mainnet, Ankr for base.
"""
import asyncio
import logging
from typing import Dict, List, Callable
from datetime import datetime
from pathlib import Path
import json
import sqlite3

logger = logging.getLogger(__name__)

class AlertManager:
    def __init__(self, config: dict):
        self.config = config
        self.ankr_key = config.get('ankr',{}).get('api_key','')
        self.goldrush_key = config.get('goldrush',{}).get('api_key','')
        self.db_path = config.get('portfolio',{}).get('database_path','data/trades.db')
        self.watched: Dict[str, Dict] = {}
        self.running = False
        self.callbacks: List[Callable] = []
        self.tg_bot = None
        self._init_db()

    def _init_db(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(self.db_path)
        con.execute("""
            CREATE TABLE IF NOT EXISTS watched_wallets (
                wallet TEXT PRIMARY KEY,
                chain TEXT,
                grade TEXT,
                score REAL,
                coin_symbol TEXT,
                source_coin TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen_tx TEXT,
                alert_count INTEGER DEFAULT 0
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS wallet_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet TEXT,
                chain TEXT,
                new_token TEXT,
                new_token_symbol TEXT,
                tx_hash TEXT,
                alert_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                grade TEXT
            )
        """)
        con.commit()
        # Load existing
        for row in con.execute("SELECT wallet, chain, grade, score, coin_symbol FROM watched_wallets"):
            self.watched[row[0].lower()] = {'wallet': row[0], 'chain': row[1], 'grade': row[2], 'score': row[3], 'coin_symbol': row[4]}
        con.close()
        logger.info(f"AlertManager loaded {len(self.watched)} watched wallets")

    async def add_watch(self, reputable: Dict):
        wallet = reputable['wallet'].lower()
        if wallet in self.watched:
            return
        self.watched[wallet] = reputable
        con = sqlite3.connect(self.db_path)
        con.execute("INSERT OR REPLACE INTO watched_wallets (wallet, chain, grade, score, coin_symbol, source_coin) VALUES (?,?,?,?,?,?)",
                    (reputable['wallet'], reputable['chain'], reputable['grade'], reputable['score'], reputable['coin_symbol'], reputable['coin']))
        con.commit()
        con.close()
        logger.info(f"Alert watch added: {reputable['wallet'][:10]}... grade {reputable['grade']} ({reputable['chain']}) via {reputable['coin_symbol']} -> ALERT if trades new token")

    def list_watched(self) -> List[Dict]:
        con = sqlite3.connect(self.db_path)
        rows = [dict(zip([c[0] for c in con.execute("SELECT * FROM watched_wallets").description], r)) for r in con.execute("SELECT * FROM watched_wallets ORDER BY added_at DESC")]
        con.close()
        return rows

    def list_alerts(self, limit: int = 50) -> List[Dict]:
        con = sqlite3.connect(self.db_path)
        rows = [dict(zip([c[0] for c in con.execute("SELECT * FROM wallet_alerts").description], r)) for r in con.execute("SELECT * FROM wallet_alerts ORDER BY alert_at DESC LIMIT ?", (limit,))]
        con.close()
        return rows

    async def start(self):
        self.running = True
        logger.info(f"AlertManager watching {len(self.watched)} wallets (poll every 20s)")
        # Load Ankr/GoldRush clients
        while self.running:
            try:
                await self._poll_all()
            except Exception as e:
                logger.debug(f"Alert poll error: {e}")
            await asyncio.sleep(20)

    async def stop(self):
        self.running = False

    async def _poll_all(self):
        # Sample up to 10 watched wallets per cycle to stay within rate limits
        wallets = list(self.watched.values())[:10]
        for w in wallets:
            await self._poll_wallet(w)
            await asyncio.sleep(0.5)

    async def _poll_wallet(self, meta: Dict):
        wallet = meta['wallet']
        chain = meta['chain']
        try:
            if chain == 'robinhood' and self.goldrush_key and not self.goldrush_key.startswith('${'):
                await self._poll_goldrush(wallet, chain, meta)
            elif self.ankr_key and not self.ankr_key.startswith('${'):
                await self._poll_ankr(wallet, chain, meta)
        except Exception as e:
            logger.debug(f"poll {wallet[:8]} error: {e}")

    async def _poll_ankr(self, wallet: str, chain: str, meta: Dict):
        from ..utils.ankr_api import AnkrAPI
        # Use try fallback for import path
        try:
            ankr = AnkrAPI(self.ankr_key)
        except:
            from src.utils.ankr_api import AnkrAPI
            ankr = AnkrAPI(self.ankr_key)
        ankr_chain = 'base' if chain=='base' else 'eth'
        txs = await ankr.get_transactions(wallet, ankr_chain, page_size=5)
        if not txs: return
        latest = txs[0].get('hash','')
        # Check if new tx since last seen
        con = sqlite3.connect(self.db_path)
        row = con.execute("SELECT last_seen_tx FROM watched_wallets WHERE wallet=?", (wallet,)).fetchone()
        last = row[0] if row else None
        if latest and latest != last:
            # Check if tx involves a new token (token_transfers)
            transfers = await ankr.get_token_transfers(wallet, ankr_chain, page_size=5)
            new_token = None
            for t in transfers:
                # Heuristic: token not in watched coin list
                if t.get('token_address','').lower() != meta.get('coin','').lower():
                    new_token = t.get('token_address')
                    symbol = t.get('token_symbol','???')
                    break
            if new_token:
                await self._fire_alert(wallet, chain, meta, new_token, symbol, latest)
            # Update last_seen
            con.execute("UPDATE watched_wallets SET last_seen_tx=?, alert_count=alert_count+1 WHERE wallet=?", (latest, wallet))
            con.commit()
        con.close()

    async def _poll_goldrush(self, wallet: str, chain: str, meta: Dict):
        # GoldRush transactions_v3 for robinhood-mainnet
        import aiohttp
        url = f"https://api.covalenthq.com/v1/robinhood-mainnet/address/{wallet}/transactions_v3/?page-size=5&key={self.goldrush_key}"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200: return
                data = await r.json()
                items = data.get('data',{}).get('items',[])[:1]
                if not items: return
                tx_hash = items[0].get('tx_hash','')
                con = sqlite3.connect(self.db_path)
                row = con.execute("SELECT last_seen_tx FROM watched_wallets WHERE wallet=?", (wallet,)).fetchone()
                last = row[0] if row else None
                if tx_hash and tx_hash != last:
                    new_token = None
                    symbol = '???'
                    for log in items[0].get('log_events',[]):
                        dec = log.get('decoded',{})
                        if dec and dec.get('name') == 'Transfer':
                            params = {p.get('name'): p.get('value') for p in dec.get('params',[])}
                            to_addr = params.get('to', '') or params.get('to_address','')
                            if to_addr and to_addr.lower() == wallet.lower():
                                new_token = log.get('sender_address','')
                                # Try to get symbol from decoded
                                symbol = dec.get('symbol', '???') or '???'
                                break
                    if new_token:
                        await self._fire_alert(wallet, chain, meta, new_token, symbol, tx_hash)
                    con.execute("UPDATE watched_wallets SET last_seen_tx=? WHERE wallet=?", (tx_hash, wallet))
                    con.commit()
                con.close()

    async def _fire_alert(self, wallet: str, chain: str, meta: Dict, new_token: str, symbol: str, tx_hash: str):
        con = sqlite3.connect(self.db_path)
        con.execute("INSERT INTO wallet_alerts (wallet, chain, new_token, new_token_symbol, tx_hash, grade) VALUES (?,?,?,?,?,?)",
                    (wallet, chain, new_token, symbol, tx_hash, meta.get('grade','')))
        con.commit()
        con.close()
        msg = f"REPUTABLE {meta.get('grade','')} wallet {wallet[:10]}... ({chain}/{meta.get('coin_symbol','')}) traded NEW token {new_token[:10]}... ({symbol}) tx {tx_hash[:10]}..."
        logger.warning(msg)
        # TG gets emoji, log stays ascii to avoid cp1252 crash
        try:
            if self.tg_bot:
                self.tg_bot.broadcast(f"🚨 <b>WATCHED WALLET TRADED</b>\nGrade {meta.get('grade','')} {wallet[:6]}...{wallet[-4:]} {chain} via {meta.get('coin_symbol','')}\nNew token <code>{new_token}</code> ({symbol})\n<a href='https://dexscreener.com/{chain}/{new_token}'>dex</a> tx <code>{tx_hash[:12]}...</code>")
        except: pass
        for cb in self.callbacks:
            try: await cb({'wallet': wallet, 'chain': chain, 'new_token': new_token, 'symbol': symbol, 'tx': tx_hash, 'grade': meta.get('grade')})
            except: pass

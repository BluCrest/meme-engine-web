"""Memory - journal + pattern store for meme training.
Paper trades -> features -> sqlite + jsonl, queried before buys, retrained via mini-transformer.
"""
import sqlite3
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import hashlib

logger = logging.getLogger(__name__)

class Memory:
    def __init__(self, db_path: str = "data/trades.db", journal_path: str = "data/memory.jsonl"):
        self.db_path = db_path
        self.journal_path = Path(journal_path)
        self.journal_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        con = sqlite3.connect(self.db_path)
        con.execute("""
            CREATE TABLE IF NOT EXISTS memory_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address TEXT,
                chain TEXT,
                entry_price REAL,
                exit_price REAL,
                pnl_percent REAL,
                hold_minutes REAL,
                features TEXT,
                label INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Add live mcap/profit columns if missing (migrate)
        for col, typ in [("entry_mcap","REAL"),("exit_mcap","REAL"),("buy_amount","REAL"),("profit_usd","REAL"),("x_return","REAL")]:
            try: con.execute(f"ALTER TABLE memory_trades ADD COLUMN {col} {typ}")
            except: pass
        con.commit()
        con.close()

    def _featurize(self, token_address: str, chain: str, entry_price: float, liquidity: float = 0,
                   mcap: float = 0, age_minutes: float = 0, dev_score: float = 50,
                   holders_score: float = 50, volume_1h: float = 0, scout_score: float = 50) -> Dict:
        # Deterministic 12-dim feature vector for meme (no external embedding yet)
        # Will be fed to mini-transformer later
        return {
            "liquidity_log": float(__import__('math').log10(max(liquidity, 1000))),
            "mcap_log": float(__import__('math').log10(max(mcap, 1000))),
            "age_minutes": float(age_minutes),
            "dev_score": float(dev_score)/100,
            "holders_score": float(holders_score)/100,
            "volume_1h_log": float(__import__('math').log10(max(volume_1h, 100))),
            "scout_score": float(scout_score)/100,
            "chain_base": 1 if chain=='base' else 0,
            "chain_robinhood": 1 if chain=='robinhood' else 0,
            "chain_solana": 1 if chain=='solana' else 0,
            "price": float(entry_price),
            # Hash-based pseudo-embedding for token address (stable)
            "token_hash": int(hashlib.md5(token_address.encode()).hexdigest()[:4], 16) / 65535,
            "bias": 1.0
        }

    def record_open(self, token_address: str, chain: str, entry_price: float, features: Dict = None, entry_mcap: float = 0, buy_amount: float = 0):
        # Write to journal on open with LIVE mcap stamped (not synthetic)
        rec = {"event": "open", "token": token_address, "chain": chain, "entry_price": entry_price, "entry_mcap": entry_mcap, "buy_amount": buy_amount, "features": features or {}, "ts": datetime.now().isoformat()}
        with open(self.journal_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(rec) + "\n")

    def record_close(self, token_address: str, chain: str, entry_price: float, exit_price: float,
                     hold_minutes: float = 0, features: Dict = None, entry_mcap: float = 0, exit_mcap: float = 0,
                     buy_amount: float = 0, token_amount: float = 0):
        # LIVE maths: ROI, x, profit_usd stamped with live prices/mcaps (only paper money is demo)
        # If buy $100 at 0.0001 and exit 0.0005 (5x): x=5.0 ROI 400% profit $400 total $500
        x = (exit_price / entry_price) if entry_price else 0
        roi = ((exit_price - entry_price)/entry_price*100) if entry_price else 0
        profit_usd = (token_amount * exit_price - buy_amount) if token_amount and buy_amount else (buy_amount * (x - 1) if buy_amount else 0)
        pnl = roi
        label = 1 if roi > 50 else 0
        feats = features or self._featurize(token_address, chain, entry_price)
        con = sqlite3.connect(self.db_path)
        con.execute("INSERT INTO memory_trades (token_address, chain, entry_price, exit_price, pnl_percent, hold_minutes, features, label, entry_mcap, exit_mcap, buy_amount, profit_usd, x_return) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (token_address, chain, entry_price, exit_price, pnl, hold_minutes, json.dumps(feats), label, entry_mcap, exit_mcap, buy_amount, profit_usd, x))
        con.commit()
        con.close()
        rec = {"event": "close", "token": token_address, "chain": chain, "entry": entry_price, "exit": exit_price, "entry_mcap": entry_mcap, "exit_mcap": exit_mcap, "buy_amount": buy_amount, "profit_usd": profit_usd, "x": round(x,3), "roi": round(roi,2), "pnl": round(pnl,2), "label": label, "features": feats, "ts": datetime.now().isoformat()}
        with open(self.journal_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(rec) + "\n")
        logger.info(f"Memory close {token_address[:8]}... live entry ${entry_price:.8f} mcap ${entry_mcap:.0f} -> exit ${exit_price:.8f} mcap ${exit_mcap:.0f} x{x:.2f} ROI {roi:.1f}% profit ${profit_usd:.2f} label {label}")

    def query_similar(self, features: Dict, k: int = 5) -> Dict:
        """Pattern recognition: find k most similar past trades by Euclidean distance on features."""
        try:
            con = sqlite3.connect(self.db_path)
            rows = list(con.execute("SELECT features, pnl_percent, label FROM memory_trades ORDER BY id DESC LIMIT 200"))
            con.close()
            if not rows:
                return {"found": 0, "win_rate": 0.5, "avg_pnl": 0, "prediction": 0.5}
            import math
            # Simple L2 distance on feature vectors
            def dist(a, b):
                keys = set(a.keys()) & set(b.keys())
                return math.sqrt(sum((a[k]-b[k])**2 for k in keys))
            scored = []
            for feat_json, pnl, label in rows:
                try:
                    past = json.loads(feat_json)
                    d = dist(features, past)
                    scored.append((d, pnl, label))
                except: continue
            scored.sort(key=lambda x: x[0])
            top = scored[:k]
            if not top:
                return {"found": 0, "win_rate": 0.5, "avg_pnl": 0, "prediction": 0.5}
            win_rate = sum(1 for _,_,l in top if l==1)/len(top)
            avg_pnl = sum(p for _,p,_ in top)/len(top)
            # Prediction = win_rate adjusted by distance (closer = more weight)
            prediction = win_rate
            return {"found": len(top), "win_rate": win_rate, "avg_pnl": avg_pnl, "prediction": prediction, "neighbors": top}
        except Exception as e:
            logger.debug(f"Memory query error: {e}")
            return {"found": 0, "win_rate": 0.5, "avg_pnl": 0, "prediction": 0.5}

    def stats(self) -> Dict:
        con = sqlite3.connect(self.db_path)
        row = con.execute("SELECT COUNT(*), AVG(pnl_percent), SUM(CASE WHEN label=1 THEN 1 ELSE 0 END) FROM memory_trades").fetchone()
        con.close()
        total = row[0] or 0
        avg = row[1] or 0
        wins = row[2] or 0
        return {"total_memories": total, "avg_pnl": avg, "win_rate": wins/total if total else 0, "ready_for_transformer": total >= 50}

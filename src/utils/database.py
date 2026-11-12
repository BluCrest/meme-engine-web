import sqlite3
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

class TradeDatabase:
    """SQLite database for tracking trades and positions"""
    
    def __init__(self, db_path: str = "data/trades.db", initial_capital: float = 10000.0):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.initial_capital = float(initial_capital)
        self._create_tables()
    
    def _create_tables(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address TEXT NOT NULL,
                token_symbol TEXT,
                chain TEXT NOT NULL,
                entry_price REAL NOT NULL,
                entry_amount_usd REAL NOT NULL,
                token_amount REAL NOT NULL,
                entry_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                exit_price REAL,
                exit_amount_usd REAL,
                exit_time TIMESTAMP,
                status TEXT DEFAULT 'open',
                exit_reason TEXT,
                pnl_usd REAL,
                pnl_percent REAL,
                tier_exits TEXT DEFAULT '[]'
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address TEXT NOT NULL,
                chain TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                signal_data TEXT,
                score REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                acted_on BOOLEAN DEFAULT 0
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS config_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT NOT NULL,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS equity_curve (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                equity REAL NOT NULL,
                daily_pnl REAL DEFAULT 0
            )
        """)
        # Ensure equity_curve seeded with initial capital row if empty
        cursor.execute("SELECT COUNT(*) as c FROM equity_curve")
        if cursor.fetchone()['c'] == 0:
            cursor.execute("INSERT INTO equity_curve (equity, daily_pnl) VALUES (?, 0)", (self.initial_capital,))
        self.conn.commit()
    
    def open_position(self, token_address: str, token_symbol: str, chain: str,
                      entry_price: float, entry_amount_usd: float, token_amount: float) -> int:
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO positions (token_address, token_symbol, chain, entry_price, 
                                   entry_amount_usd, token_amount, status)
            VALUES (?, ?, ?, ?, ?, ?, 'open')
        """, (token_address, token_symbol, chain, entry_price, entry_amount_usd, token_amount))
        self.conn.commit()
        return cursor.lastrowid
    
    def close_position(self, position_id: int, exit_price: float, exit_amount_usd: float, 
                       exit_reason: str, tier_exits: list = None):
        cursor = self.conn.cursor()
        cursor.execute("SELECT entry_amount_usd FROM positions WHERE id = ?", (position_id,))
        row = cursor.fetchone()
        if not row:
            return
        
        entry_amount = row['entry_amount_usd']
        pnl_usd = exit_amount_usd - entry_amount
        pnl_percent = (pnl_usd / entry_amount) * 100 if entry_amount > 0 else 0
        
        cursor.execute("""
            UPDATE positions SET exit_price = ?, exit_amount_usd = ?, exit_time = CURRENT_TIMESTAMP,
                   status = 'closed', exit_reason = ?, pnl_usd = ?, pnl_percent = ?, tier_exits = ?
            WHERE id = ?
        """, (exit_price, exit_amount_usd, exit_reason, pnl_usd, pnl_percent, 
              json.dumps(tier_exits or []), position_id))
        self.conn.commit()
    
    def update_position_tier(self, position_id: int, tier_exits: list):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE positions SET tier_exits = ? WHERE id = ?",
                       (json.dumps(tier_exits), position_id))
        self.conn.commit()
    
    def get_open_positions(self, chain: str = None) -> List[Dict]:
        cursor = self.conn.cursor()
        if chain:
            cursor.execute("SELECT * FROM positions WHERE status = 'open' AND chain = ?", (chain,))
        else:
            cursor.execute("SELECT * FROM positions WHERE status = 'open'")
        return [dict(row) for row in cursor.fetchall()]
    
    def get_position(self, position_id: int) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM positions WHERE id = ?", (position_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def log_signal(self, token_address: str, chain: str, signal_type: str, 
                   signal_data: dict, score: float = 0):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO signals (token_address, chain, signal_type, signal_data, score)
            VALUES (?, ?, ?, ?, ?)
        """, (token_address, chain, signal_type, json.dumps(signal_data), score))
        self.conn.commit()
    
    def get_pnl_history(self) -> List[float]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT pnl_percent/100.0 as r FROM positions WHERE status='closed' ORDER BY exit_time")
        return [row['r'] for row in cursor.fetchall() if row['r'] is not None]

    def record_equity(self, equity: float):
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO equity_curve (equity) VALUES (?)", (equity,))
        self.conn.commit()

    def get_equity_curve(self) -> pd.Series:
        cursor = self.conn.cursor()
        cursor.execute("SELECT equity FROM equity_curve ORDER BY id")
        vals = [row['equity'] for row in cursor.fetchall()]
        if not vals:
            return pd.Series([10000.0])
        return pd.Series(vals, dtype=float)

    def get_current_equity(self, initial_capital: float = 10000.0) -> float:
        # equity = initial + sum closed pnl + unrealized (ignored for kill switch, closed only)
        cursor = self.conn.cursor()
        cursor.execute("SELECT SUM(pnl_usd) as s FROM positions WHERE status='closed'")
        s = cursor.fetchone()['s']
        return float(initial_capital + (s or 0))

    def get_stats(self) -> Dict:
        cursor = self.conn.cursor()
        
        cursor.execute("SELECT COUNT(*) as total, SUM(pnl_usd) as total_pnl FROM positions WHERE status = 'closed'")
        closed = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(*) as open_count FROM positions WHERE status = 'open'")
        open_count = cursor.fetchone()
        
        cursor.execute("SELECT AVG(pnl_percent) as avg_pnl FROM positions WHERE status = 'closed' AND pnl_percent > 0")
        avg_win = cursor.fetchone()
        
        cursor.execute("SELECT AVG(pnl_percent) as avg_pnl FROM positions WHERE status = 'closed' AND pnl_percent <= 0")
        avg_loss = cursor.fetchone()
        
        cursor.execute("""
            SELECT exit_reason, COUNT(*) as count 
            FROM positions WHERE status = 'closed' 
            GROUP BY exit_reason
        """)
        exit_reasons = {row['exit_reason']: row['count'] for row in cursor.fetchall()}
        
        total = closed['total'] or 0
        wins = 0
        if total > 0:
            cursor.execute("SELECT COUNT(*) as wins FROM positions WHERE status = 'closed' AND pnl_percent > 0")
            wins = cursor.fetchone()['wins']
        
        # Quant-bot style stats: Sharpe/Sortino/Calmar/maxDD via equity curve
        try:
            from ..math.stats import max_drawdown, sharpe, sortino, calmar, cagr, returns_of
        except ImportError:
            try:
                from math.stats import max_drawdown, sharpe, sortino, calmar, cagr, returns_of
            except:
                max_drawdown = sharpe = sortino = calmar = cagr = returns_of = None
        equity = self.get_equity_curve()
        extra = {}
        if equity is not None and len(equity) >= 3 and returns_of:
            rets = returns_of(equity)
            dd, _ = max_drawdown(equity)
            extra = {
                'equity': float(equity.iloc[-1]),
                'max_drawdown_pct': float(dd*100),
                'sharpe': float(sharpe(rets) if rets is not None else 0),
                'sortino': float(sortino(rets) if rets is not None else 0),
                'cagr_pct': float(cagr(equity)*100 if cagr else 0),
                'calmar': float(calmar(equity) if calmar else 0),
            }
        
        pnls = self.get_pnl_history()
        win_rate = (wins / total * 100) if total > 0 else 0
        payoff = 0
        if total > 0 and avg_win['avg_pnl'] and avg_loss['avg_pnl']:
            payoff = abs((avg_win['avg_pnl'] or 0) / max(abs(avg_loss['avg_pnl'] or 1), 1))
        
        return {
            'total_trades': total,
            'open_positions': open_count['open_count'],
            'total_pnl_usd': closed['total_pnl'] or 0,
            'win_rate': win_rate,
            'payoff_ratio': float(payoff),
            'avg_win_percent': avg_win['avg_pnl'] or 0,
            'avg_loss_percent': avg_loss['avg_pnl'] or 0,
            'exit_reasons': exit_reasons,
            **extra
        }
    
    def close(self):
        self.conn.close()

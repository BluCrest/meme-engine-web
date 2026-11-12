"""Phase2 Memory Log - token lifecycles for transformer training.
Logs per token: Alpha Trigger, Liquidity Profile, 5s price ticks x360 (30m), Final Outcome.
Schema matches user Phase2 spec: SQLite token_lifecycles for 10k lifecycles.
"""
import sqlite3
import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List

logger = logging.getLogger(__name__)

# Python data logging schema template (user requested)
LOGGING_SCHEMA = """
-- token_lifecycles: one row per meme, 30m of 5s ticks + outcome
CREATE TABLE token_lifecycles (
    token_address TEXT PRIMARY KEY,
    chain TEXT NOT NULL,                 -- base / robinhood-mainnet
    created_at TIMESTAMP,                -- pairCreatedAt
    deployer TEXT,                       -- dev wallet
    first_buyer TEXT,                    -- alpha trigger: who bought first
    first_buyer_is_smart BOOLEAN,        -- smart money vs unknown
    initial_liquidity_usd REAL,          -- liquidity profile
    liquidity_locked BOOLEAN,            -- locked/burned?
    price_curve TEXT,                    -- JSON [360] 5s ticks for 30m
    volume_curve TEXT,                   -- JSON [360] buy volume per 5s
    final_mcap REAL,                     -- final outcome mcap
    final_price REAL,
    outcome TEXT,                        -- 'runner_1M' / 'rug_0' / 'dead'
    label INTEGER,                       -- 1=runner, 0=rug (for transformer)
    created_at_db TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Example insert:
-- INSERT INTO token_lifecycles (token_address, chain, price_curve, outcome) VALUES ('0x...', 'robinhood-mainnet', '[0.0001,0.00012,...]', 'runner_1M')
"""

class LifecycleLogger:
    def __init__(self, db_path: str = "data/trades.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self.active: Dict[str, Dict] = {}  # token -> tracker
        self.running = False

    def _init_db(self):
        con = sqlite3.connect(self.db_path)
        con.execute("""
            CREATE TABLE IF NOT EXISTS token_lifecycles (
                token_address TEXT PRIMARY KEY,
                chain TEXT NOT NULL,
                created_at TIMESTAMP,
                deployer TEXT,
                first_buyer TEXT,
                first_buyer_is_smart BOOLEAN,
                initial_liquidity_usd REAL,
                liquidity_locked BOOLEAN,
                price_curve TEXT,
                volume_curve TEXT,
                final_mcap REAL,
                final_price REAL,
                outcome TEXT,
                label INTEGER,
                created_at_db TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.commit()
        con.close()

    async def track_token(self, token_address: str, chain: str, deployer: str = None, initial_liquidity: float = 0):
        """Start 30m 5s tick logging for a new token (Phase2)."""
        if token_address.lower() in self.active:
            return
        self.active[token_address.lower()] = {
            'token': token_address,
            'chain': chain,
            'deployer': deployer,
            'initial_liquidity': initial_liquidity,
            'price_curve': [],
            'volume_curve': [],
            'start': datetime.now(),
            'ticks': 0
        }
        logger.info(f"Lifecycle tracking {token_address[:8]}... on {chain} for 30m 5s ticks (Phase2)")
        # Spawn background ticker
        asyncio.create_task(self._ticker_loop(token_address.lower()))

    async def _ticker_loop(self, token_lower: str):
        try:
            from execution.swap_executor import SwapExecutor
            from utils.config import load_config
        except ImportError:
            from src.execution.swap_executor import SwapExecutor
            from src.utils.config import load_config
        cfg = load_config()
        executor = SwapExecutor(cfg)
        chains = cfg.get('chains', {})
        for i in range(360):  # 30m * 60/5 = 360 ticks
            if token_lower not in self.active:
                break
            info = self.active[token_lower]
            try:
                live = await executor.get_token_live_data(info['token'], info['chain'], chains)
                price = live.get('priceUsd', 0)
                vol = live.get('volume24h', 0)
                info['price_curve'].append(price)
                info['volume_curve'].append(vol)
                # Log every 60 ticks (5m) heartbeat
                if i % 60 == 0 and i>0:
                    logger.debug(f"Lifecycle {info['token'][:8]}... tick {i}/360 price ${price:.6f}")
            except Exception as e:
                logger.debug(f"Lifecycle tick error: {e}")
                info['price_curve'].append(0)
                info['volume_curve'].append(0)
            await asyncio.sleep(5)
        # After 30m, finalize outcome
        await self._finalize(token_lower)

    async def _finalize(self, token_lower: str):
        info = self.active.pop(token_lower, None)
        if not info:
            return
        try:
            try:
                from execution.swap_executor import SwapExecutor
                from utils.config import load_config
            except ImportError:
                from src.execution.swap_executor import SwapExecutor
                from src.utils.config import load_config
            cfg = load_config()
            executor = SwapExecutor(cfg)
            live = await executor.get_token_live_data(info['token'], info['chain'], cfg.get('chains',{}))
            final_mcap = live.get('mcap', 0) or live.get('fdv', 0)
            final_price = live.get('priceUsd', 0)
            # Outcome: runner if mcap >1M, rug if <10k or price 0, else dead
            if final_mcap >= 1_000_000:
                outcome, label = 'runner_1M', 1
            elif final_mcap < 10000 or final_price == 0:
                outcome, label = 'rug_0', 0
            else:
                outcome, label = 'dead', 0
            con = sqlite3.connect(self.db_path)
            con.execute("""
                INSERT OR REPLACE INTO token_lifecycles 
                (token_address, chain, created_at, deployer, initial_liquidity_usd, price_curve, volume_curve, final_mcap, final_price, outcome, label)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (info['token'], info['chain'], info['start'].isoformat(), info['deployer'], info['initial_liquidity'],
                  json.dumps(info['price_curve']), json.dumps(info['volume_curve']), final_mcap, final_price, outcome, label))
            con.commit()
            con.close()
            logger.info(f"Lifecycle finalized {info['token'][:8]}... {outcome} mcap ${final_mcap:.0f} ticks {len(info['price_curve'])}")
        except Exception as e:
            logger.error(f"Lifecycle finalize error: {e}")

    def stats(self):
        con = sqlite3.connect(self.db_path)
        row = con.execute("SELECT COUNT(*), SUM(CASE WHEN label=1 THEN 1 ELSE 0 END) FROM token_lifecycles").fetchone()
        con.close()
        total = row[0] or 0
        runners = row[1] or 0
        return {"total_lifecycles": total, "runners": runners, "rugs": total - runners, "progress_to_10k": f"{total}/10000"}

"""
Python data logging schema template - token timestamps for ML training
Copy this to capture 5s ticks for 30m per token as per Phase2 spec.
"""
import sqlite3
from datetime import datetime
import json

# Schema - run once
con = sqlite3.connect("data/trades.db")
con.execute("""
CREATE TABLE IF NOT EXISTS token_lifecycles (
    token_address TEXT PRIMARY KEY,
    chain TEXT NOT NULL,
    created_at TIMESTAMP,  -- pairCreatedAt from DexScreener GoldRush
    deployer TEXT,
    first_buyer TEXT,      -- alpha trigger
    first_buyer_is_smart BOOLEAN,
    initial_liquidity_usd REAL,
    liquidity_locked BOOLEAN,
    price_curve TEXT,      -- JSON array 360 x 5s ticks = 30m
    volume_curve TEXT,
    final_mcap REAL,
    final_price REAL,
    outcome TEXT,          -- runner_1M / rug_0 / dead
    label INTEGER          -- 1 runner, 0 rug
);
""")
con.commit()

# Example: log a new token at ingestion
def log_new_token(token, chain, deployer, initial_liq):
    con.execute("INSERT OR REPLACE INTO token_lifecycles (token_address, chain, created_at, deployer, initial_liquidity_usd, price_curve) VALUES (?,?,?,?,?,?)",
                (token, chain, datetime.now().isoformat(), deployer, initial_liq, json.dumps([])))
    con.commit()

# Every 5s for 30m: UPDATE price_curve = json_array_append
# After 30m: UPDATE final_mcap, outcome, label

# Query for transformer training:
# SELECT price_curve, volume_curve, label FROM token_lifecycles WHERE label IS NOT NULL LIMIT 10000

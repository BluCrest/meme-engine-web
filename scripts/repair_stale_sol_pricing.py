"""One-off repair: the 2026-11-16 00:16 EwLANuSw paper close was priced with the
boot fallback ($200/SOL) on entry and the live mark ($72.19) on exit, producing a
fake -58% loss while the ledger actually gained SOL.

The append-only wallet ledger is the source of truth. This script recomputes the
closed position (and its memory journal line) in SOL-consistent USD terms:

    sol_usd          = exit_price * token_amount / gross_exit_sol   (the mark the
                       exit was actually priced at)
    entry_price_usd  = gross_entry_sol * sol_usd / token_amount
    entry_amount_usd = (swap_in + gas + rent_locked) * sol_usd
    exit_amount_usd  = (swap_out - gas + rent_refund) * sol_usd
    pnl_usd          = exit_amount_usd - entry_amount_usd
    pnl_percent      = pnl_usd / entry_amount_usd * 100

Idempotent: skips when the position already shows a positive pnl.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

TOKEN = "EwLANuSwR7z9QNTXGwdsW7mFzXPiaucUBZjiXfw6pump"
LAMPORTS_PER_SOL = 1_000_000_000


def ledger_flows(ledger: sqlite3.Connection, trade_id: str) -> dict:
    rows = ledger.execute(
        "SELECT asset_mint, delta_atomic, entry_type FROM wallet_ledger "
        "WHERE trade_id = ? ORDER BY id",
        (trade_id,),
    ).fetchall()
    if not rows:
        raise SystemExit(f"no ledger rows for {trade_id}")
    flows = {
        "sol_in": 0, "sol_out": 0, "gas_buy": 0, "gas_sell": 0,
        "rent_lock": 0, "rent_refund": 0, "token_in": 0, "token_out": 0,
    }
    sell_seen = False
    for asset, delta, entry in rows:
        if entry == "SWAP_OUT" and asset != "SOL":
            sell_seen = True
        if asset == "SOL":
            if entry == "SWAP_OUT" and delta < 0:
                flows["sol_in"] += -delta
            elif entry == "SWAP_IN" and delta > 0:
                flows["sol_out"] += delta
            elif entry == "GAS_FEE":
                flows["gas_sell" if sell_seen else "gas_buy"] += -delta
            elif entry == "ATA_RENT_LOCK":
                flows["rent_lock"] -= delta
            elif entry == "ATA_RENT_REFUND":
                flows["rent_refund"] += delta
        elif entry == "SWAP_IN":
            flows["token_in"] += delta
        elif entry == "SWAP_OUT":
            flows["token_out"] += -delta
    return flows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="/app/data")
    ap.add_argument("--trades", default=None)
    ap.add_argument("--ledger", default=None)
    ap.add_argument("--memory", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    base = Path(args.data_dir)
    trades_path = Path(args.trades) if args.trades else base / "trades.db"
    ledger_path = Path(args.ledger) if args.ledger else base / "wallet_ledger.db"
    memory_path = Path(args.memory) if args.memory else base / "memory.jsonl"

    trades = sqlite3.connect(str(trades_path), timeout=15)
    trades.row_factory = sqlite3.Row
    ledger = sqlite3.connect(str(ledger_path), timeout=15)

    pos = trades.execute(
        "SELECT * FROM positions WHERE token_address = ? AND status = 'closed'", (TOKEN,)
    ).fetchone()
    if pos is None:
        raise SystemExit("target position not found")
    pos = dict(pos)
    if (pos["pnl_usd"] or 0) > 0:
        print(f"already repaired (pnl_usd={pos['pnl_usd']}); nothing to do")
        return

    trade_id = pos["wallet_trade_id"]
    f = ledger_flows(ledger, trade_id)
    tokens = f["token_in"] / (10 ** int(pos["token_decimals"]))
    if abs(tokens - float(pos["token_amount"])) > 0.01:
        print(f"warning: token amount mismatch ledger={tokens} db={pos['token_amount']}")
    gross_exit_sol = f["sol_out"] / LAMPORTS_PER_SOL
    sol_usd = float(pos["exit_price"]) * tokens / gross_exit_sol

    entry_price = f["sol_in"] / LAMPORTS_PER_SOL * sol_usd / tokens
    exit_price = float(pos["exit_price"])
    entry_usd = (f["sol_in"] + f["gas_buy"] + f["rent_lock"]) / LAMPORTS_PER_SOL * sol_usd
    exit_usd = (f["sol_out"] - f["gas_sell"] + f["rent_refund"]) / LAMPORTS_PER_SOL * sol_usd
    pnl_usd = exit_usd - entry_usd
    pnl_pct = pnl_usd / entry_usd * 100
    x = exit_price / entry_price
    roi = (x - 1) * 100

    print(f"sol_usd basis     : ${sol_usd:.4f}")
    print(f"entry {entry_price:.10f} -> exit {exit_price:.10f}  x={x:.4f}")
    print(f"entry ${entry_usd:.4f} -> exit ${exit_usd:.4f}  pnl ${pnl_usd:+.4f} ({pnl_pct:+.2f}%)")
    print(f"was: entry ${pos['entry_amount_usd']:.4f} pnl ${pos['pnl_usd']:+.4f} ({pos['pnl_percent']:+.2f}%)")
    if args.dry_run:
        return

    with trades:
        trades.execute(
            """UPDATE positions SET entry_price=?, entry_amount_usd=?, exit_price=?,
                   exit_amount_usd=?, pnl_usd=?, pnl_percent=? WHERE id=?""",
            (entry_price, entry_usd, exit_price, exit_usd, pnl_usd, pnl_pct, pos["id"]),
        )

    lines = memory_path.read_text(encoding="utf-8").splitlines()
    out, fixed = [], 0
    for line in lines:
        if not line.strip():
            continue
        rec = json.loads(line)
        if rec.get("event") == "close" and rec.get("token") == TOKEN:
            rec.update({
                "entry": entry_price,
                "exit": exit_price,
                "buy_amount": entry_usd,
                "profit_usd": pnl_usd,
                "x": round(x, 4),
                "roi": round(roi, 2),
                "pnl": round(roi, 2),
                "label": 1 if roi > 0 else 0,
            })
            if isinstance(rec.get("features"), dict):
                rec["features"]["price"] = entry_price
            fixed += 1
        out.append(json.dumps(rec))
    tmp = memory_path.with_suffix(".tmp")
    tmp.write_text("\n".join(out) + "\n", encoding="utf-8")
    tmp.replace(memory_path)
    print(f"repaired positions id={pos['id']} and {fixed} memory line(s)")


if __name__ == "__main__":
    main()

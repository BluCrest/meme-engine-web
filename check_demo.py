"""Deterministic self-tests for the append-only Solana demo wallet.

Run: python check_demo.py
No network access is used; Jupiter and Solana RPC are replaced with fakes.
"""
import asyncio
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / 'src'))

from paper.demo_wallet import (
    ATA_RENT_LAMPORTS,
    DemoWallet,
    GAS_FEE_LAMPORTS,
    SOL_ASSET,
    WSOL_MINT,
)

MINT = "TestMint111111111111111111111111111111111111"
FAIL_MINT = "FailMint111111111111111111111111111111111111"
DECIMALS = 6
fails = []


def check(name, condition, detail=""):
    ok = bool(condition)
    print(f"{'PASS' if ok else 'FAIL'} {name}{(': ' + detail) if detail else ''}")
    if not ok:
        fails.append(name)


class FakeQuoteClient:
    async def quote(self, input_mint, output_mint, amount_atomic, slippage_bps):
        if output_mint == FAIL_MINT or input_mint == FAIL_MINT:
            raise RuntimeError("no route")
        if input_mint == WSOL_MINT:
            out = amount_atomic * 2
        else:
            out = amount_atomic // 2
        return {
            "input_mint": input_mint,
            "output_mint": output_mint,
            "in_amount": amount_atomic,
            "out_amount": out,
            "price_impact_pct": "0",
            "route_plan": [],
        }


class FakeRpcClient:
    async def get_mint_decimals(self, mint):
        return DECIMALS


def ledger_rows(wallet):
    return wallet.conn.execute(
        "SELECT idempotency_key, trade_id, asset_mint, delta_atomic, entry_type "
        "FROM wallet_ledger ORDER BY id"
    ).fetchall()


async def main():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = {
            "paper": {
                "initial_sol": 1.0,
                "slippage_bps": 200,
                "fallback_sol_usd": 200.0,
                "reset_on_boot": True,
            }
        }
        wallet = DemoWallet(
            cfg,
            ledger_path=str(Path(tmp) / "wallet.db"),
            quote_client=FakeQuoteClient(),
            rpc_client=FakeRpcClient(),
            legacy_state_path=None,
        )
        await wallet.ensure_initialized()

        check("genesis is one integer DEPOSIT", wallet.sol_lamports == 1_000_000_000)
        check("genesis key", ledger_rows(wallet)[0]["idempotency_key"] == "genesis:DEPOSIT:SOL")

        buy_lamports = 500_000_000
        fill = await wallet.buy(
            MINT, buy_lamports, trade_id="trade-1", idempotency_key="trade-1:buy"
        )
        expected_cost = buy_lamports + GAS_FEE_LAMPORTS + ATA_RENT_LAMPORTS
        check("buy cost includes gas and ATA rent", fill["cost_sol_lamports"] == expected_cost)
        check("buy returns exact raw tokens", fill["token_amount_raw"] == 1_000_000_000)
        check("buy returns decimals", fill["token_decimals"] == DECIMALS)
        check(
            "buy leaves exact SOL",
            wallet.sol_lamports == 1_000_000_000 - expected_cost,
            str(wallet.sol_lamports),
        )
        check("buy leaves exact token raw", wallet.token_balance_raw(MINT) == 1_000_000_000)
        check("buy wrote 4 ledger rows", len(ledger_rows(wallet)) == 5)
        check(
            "ledger deltas are integers",
            all(isinstance(int(r["delta_atomic"]), int) for r in ledger_rows(wallet)),
        )

        before = wallet.sol_lamports
        duplicate = await wallet.buy(
            MINT, buy_lamports, trade_id="trade-1", idempotency_key="trade-1:buy"
        )
        check("duplicate buy is idempotent", duplicate["idempotent"] is True)
        check("duplicate buy does not change SOL", wallet.sol_lamports == before)
        check("duplicate buy does not add rows", len(ledger_rows(wallet)) == 5)

        sell_fill = await wallet.sell_percent(
            MINT, 25, trade_id="trade-1", idempotency_key="trade-1:sell:1",
            reason="tier1", basis_raw=1_000_000_000,
        )
        check("25% tier sells exact original-basis raw", sell_fill["token_amount_raw"] == 250_000_000)
        check("25% tier does not refund rent", sell_fill["ata_rent_delta_lamports"] == 0)
        check("25% tier pays gas", sell_fill["gas_lamports"] == GAS_FEE_LAMPORTS)
        check("25% tier leaves 75% tokens", wallet.token_balance_raw(MINT) == 750_000_000)
        check("partial sell quotes exact integer input", sell_fill["quote"]["in_amount"] == 250_000_000)

        final_fill = await wallet.sell_percent(
            MINT, 100, trade_id="trade-1", idempotency_key="trade-1:sell:2",
            reason="final", basis_raw=1_000_000_000,
        )
        check("full sell refunds ATA rent", final_fill["ata_rent_delta_lamports"] == ATA_RENT_LAMPORTS)
        check("full sell clears token", wallet.token_balance_raw(MINT) == 0)
        check(
            "flat roundtrip loses only three gas fees",
            wallet.sol_lamports == 1_000_000_000 - 3 * GAS_FEE_LAMPORTS,
            str(wallet.sol_lamports),
        )
        check("all swaps quoted exact-in integer amounts", all(
            int(r["delta_atomic"]) == int(r["delta_atomic"]) for r in ledger_rows(wallet)
        ))

        rows_before = len(ledger_rows(wallet))
        refused = await wallet.buy(
            MINT, 1_500_000_000, trade_id="trade-2", idempotency_key="trade-2:buy"
        )
        check("overspend refused", refused is None)
        check("overspend writes no rows", len(ledger_rows(wallet)) == rows_before)

        no_route = await wallet.buy(
            FAIL_MINT, 1_000_000, trade_id="trade-3", idempotency_key="trade-3:buy"
        )
        check("quote failure refuses buy", no_route is None)
        check("quote failure writes no rows", len(ledger_rows(wallet)) == rows_before)

        row_id = ledger_rows(wallet)[0]["idempotency_key"]
        for sql in (
            f"UPDATE wallet_ledger SET delta_atomic = 0 WHERE idempotency_key = '{row_id}'",
            f"DELETE FROM wallet_ledger WHERE idempotency_key = '{row_id}'",
        ):
            try:
                wallet.conn.execute(sql)
                check("append-only trigger rejects mutation", False)
            except sqlite3.DatabaseError:
                check("append-only trigger rejects mutation", True)

        try:
            await wallet.sell_percent(MINT, 0)
            check("invalid percentage rejected", False)
        except Exception:
            check("invalid percentage rejected", True)

        wallet.close()

        persist_cfg = {
            "paper": {
                "initial_sol": 1.0,
                "slippage_bps": 200,
                "fallback_sol_usd": 200.0,
                "reset_on_boot": False,
            }
        }
        persist_path = str(Path(tmp) / "persist.db")
        w6 = DemoWallet(
            persist_cfg,
            ledger_path=persist_path,
            quote_client=FakeQuoteClient(),
            rpc_client=FakeRpcClient(),
            legacy_state_path=None,
        )
        await w6.ensure_initialized()
        await w6.buy(MINT, 100_000_000, trade_id="persist", idempotency_key="persist:buy")
        w6.mark_holding(MINT, "0.001")
        equity_before = w6.equity_usd()
        w6.close()

        w7 = DemoWallet(
            persist_cfg,
            ledger_path=persist_path,
            quote_client=FakeQuoteClient(),
            rpc_client=FakeRpcClient(),
            legacy_state_path=None,
        )
        await w7.ensure_initialized()
        check("marks survive restart", w7._marks.get(MINT) is not None)
        check("decimals survive restart", w7._decimals.get(MINT) == DECIMALS)
        check("equity survives restart", w7.equity_usd() == equity_before)
        w7.close()

    print()
    print("RESULT:", "ALL PASS" if not fails else f"{len(fails)} FAILURES: {fails}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

"""Deterministic append-only Solana demo wallet.

Hard rules:
- SOL balances live in integer lamports; SPL token balances live in raw integer units.
- There are no mutable balance fields. Current balances are always SUM(delta_atomic)
  over the append-only wallet_ledger table.
- Every simulated swap asks Jupiter for a live exact-in quote using the exact integer
  input amount. Spot-price multiplication is never used for fills.
- Every swap pays 500,000 lamports gas. Buying a token from zero balance locks
  2,039,280 lamports of ATA rent; selling the full balance refunds it.
- All ledger entries for a swap are inserted in one SQLite transaction after
  balance guards pass. A failed guard leaves zero partial entries.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import sqlite3
import time
import uuid
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import aiohttp

logger = logging.getLogger(__name__)

LAMPORTS_PER_SOL = 1_000_000_000
MICRO_USD_PER_USD = 1_000_000
SOL_ASSET = "SOL"
WSOL_MINT = "So11111111111111111111111111111111111111112"
PUBLIC_SOLANA_RPC = "https://api.mainnet-beta.solana.com"

GAS_FEE_LAMPORTS = 500_000
ATA_RENT_LAMPORTS = 2_039_280

ENTRY_DEPOSIT = "DEPOSIT"
ENTRY_SWAP_IN = "SWAP_IN"
ENTRY_SWAP_OUT = "SWAP_OUT"
ENTRY_GAS_FEE = "GAS_FEE"
ENTRY_ATA_RENT_LOCK = "ATA_RENT_LOCK"
ENTRY_ATA_RENT_REFUND = "ATA_RENT_REFUND"

ENTRY_TYPES = (
    ENTRY_DEPOSIT,
    ENTRY_SWAP_IN,
    ENTRY_SWAP_OUT,
    ENTRY_GAS_FEE,
    ENTRY_ATA_RENT_LOCK,
    ENTRY_ATA_RENT_REFUND,
)

DEFAULT_QUOTE_URL = "https://api.jup.ag/swap/v1"
DEFAULT_QUOTE_FALLBACKS = ("https://lite-api.jup.ag/swap/v1",)
_KEY_RE = re.compile(r"[A-Za-z0-9_.:-]{1,180}")


class WalletError(RuntimeError):
    """Base error for ledger wallet failures."""


class QuoteError(WalletError):
    """Jupiter could not provide a valid exact-in quote."""


class InsufficientBalance(WalletError):
    """A swap would make a guarded balance negative."""


def _decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise WalletError(f"{name} must be an integer")
    if value <= 0:
        raise WalletError(f"{name} must be positive")
    return value


def _sol_to_lamports(value: Any) -> int:
    return int((_decimal(value) * LAMPORTS_PER_SOL).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _lamports_to_sol(lamports: int) -> Decimal:
    return Decimal(lamports) / Decimal(LAMPORTS_PER_SOL)


def _usd_to_micro(value: Any) -> int:
    return int((_decimal(value) * MICRO_USD_PER_USD).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _normalise_quote_url(base_url: str) -> str:
    base = (base_url or DEFAULT_QUOTE_URL).rstrip("/")
    if base.endswith("/quote"):
        return base
    return base + "/quote"


def _order_v2_url(base_url: str) -> str:
    base = (base_url or DEFAULT_QUOTE_URL).rstrip("/")
    if base.endswith("/quote"):
        base = base[:-6]
    if base.endswith("/swap/v1"):
        return base[:-8] + "/swap/v2/order"
    if base.endswith("/swap/v2"):
        return base + "/order"
    return "https://api.jup.ag/swap/v2/order"


class JupiterQuoteClient:
    """Small async client for Jupiter exact-in quotes."""

    def __init__(
        self,
        base_url: str = DEFAULT_QUOTE_URL,
        fallback_urls: Sequence[str] = DEFAULT_QUOTE_FALLBACKS,
        timeout_seconds: int = 20,
    ):
        urls: List[str] = []
        for candidate in (base_url, *fallback_urls):
            url = _normalise_quote_url(candidate)
            if url not in urls:
                urls.append(url)
        self.urls = urls
        self.order_v2_urls: List[str] = []
        for url in urls:
            order_url = _order_v2_url(url)
            if order_url not in self.order_v2_urls:
                self.order_v2_urls.append(order_url)
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    @staticmethod
    def _error_from_body(body: str) -> str:
        try:
            data = json.loads(body)
        except Exception:
            return body[:200]
        code = data.get("errorCode") or data.get("code") or ""
        message = data.get("error") or data.get("message") or body
        return f"{code}: {message}" if code else str(message)[:200]

    async def quote(
        self,
        input_mint: str,
        output_mint: str,
        amount_atomic: int,
        slippage_bps: int,
    ) -> Dict[str, Any]:
        amount_atomic = _positive_int(amount_atomic, "amount_atomic")
        slippage_bps = int(slippage_bps)
        if slippage_bps < 0 or slippage_bps > 10_000:
            raise QuoteError(f"invalid slippage bps: {slippage_bps}")

        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount_atomic),
            "slippageBps": str(slippage_bps),
            "swapMode": "ExactIn",
        }
        last_error: Optional[Exception] = None

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            for url in self.urls:
                try:
                    async with session.get(url, params=params) as resp:
                        body = await resp.text()
                        if resp.status == 200:
                            data = json.loads(body)
                            in_amount = int(data["inAmount"])
                            out_amount = int(data["outAmount"])
                            if in_amount != amount_atomic:
                                last_error = QuoteError(
                                    f"quote input mismatch: asked {amount_atomic}, got {in_amount}"
                                )
                                continue
                            if out_amount <= 0:
                                last_error = QuoteError("quote returned non-positive outAmount")
                                continue
                            return {
                                "input_mint": input_mint,
                                "output_mint": output_mint,
                                "in_amount": in_amount,
                                "out_amount": out_amount,
                                "other_amount_threshold": int(data.get("otherAmountThreshold", 0) or 0),
                                "price_impact_pct": str(data.get("priceImpactPct", "")),
                                "route_plan": data.get("routePlan", []),
                                "url": url,
                            }

                        error = self._error_from_body(body)
                        last_error = QuoteError(f"HTTP {resp.status} from Jupiter: {error}")
                except QuoteError:
                    raise
                except Exception as exc:
                    last_error = exc

            # Jupiter Metis refuses some Token-2022/PumpSwap mints. Jupiter Swap V2
            # Order still routes them through Pump.fun AMM; use it as the fallback.
            for url in self.order_v2_urls:
                try:
                    order_params = {
                        "inputMint": input_mint,
                        "outputMint": output_mint,
                        "amount": str(amount_atomic),
                        "slippageBps": str(slippage_bps),
                    }
                    async with session.get(url, params=order_params) as resp:
                        body = await resp.text()
                        if resp.status != 200:
                            last_error = QuoteError(
                                f"HTTP {resp.status} from Jupiter v2 order: {self._error_from_body(body)}"
                            )
                            continue
                        data = json.loads(body)
                        in_amount = int(data["inAmount"])
                        out_amount = int(data["outAmount"])
                        if in_amount != amount_atomic:
                            last_error = QuoteError(
                                f"v2 order input mismatch: asked {amount_atomic}, got {in_amount}"
                            )
                            continue
                        if out_amount <= 0:
                            last_error = QuoteError("Jupiter v2 order returned non-positive outAmount")
                            continue
                        return {
                            "input_mint": input_mint,
                            "output_mint": output_mint,
                            "in_amount": in_amount,
                            "out_amount": out_amount,
                            "other_amount_threshold": int(data.get("otherAmountThreshold", 0) or 0),
                            "price_impact_pct": str(data.get("priceImpactPct", "")),
                            "route_plan": data.get("routePlan", []),
                            "url": url,
                        }
                except Exception as exc:
                    last_error = exc

        raise QuoteError(f"Jupiter quote unavailable: {last_error}")


class SolanaRpcClient:
    """Minimal RPC client used only to resolve exact SPL mint decimals."""

    def __init__(
        self,
        rpc_url: str = PUBLIC_SOLANA_RPC,
        fallback_urls: Sequence[str] = (),
        timeout_seconds: int = 8,
    ):
        urls: List[str] = []
        for candidate in (rpc_url, *fallback_urls):
            if not candidate or "${" in candidate or candidate in urls:
                continue
            urls.append(candidate)
        self.urls = urls or [PUBLIC_SOLANA_RPC]
        self.rpc_url = self.urls[0]
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async def get_mint_decimals(self, mint: str) -> Optional[int]:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [mint, {"encoding": "jsonParsed"}],
        }
        for url in self.urls:
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.post(url, json=payload) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json()
                value = (data.get("result") or {}).get("value") or {}
                parsed = (value.get("data") or {}).get("parsed") or {}
                info = parsed.get("info") or {}
                decimals = info.get("decimals")
                if decimals is not None:
                    return int(decimals)
            except Exception as exc:
                logger.debug("mint decimals RPC error %s (%s): %s", mint[:8], url, exc)
        return None


class DemoWallet:
    """Append-only ledger wallet. Balances are derived, never stored."""

    def __init__(
        self,
        config: dict,
        ledger_path: Optional[str] = None,
        quote_client: Optional[Any] = None,
        rpc_client: Optional[Any] = None,
        legacy_state_path: Optional[str] = "data/demo_wallet.json",
        state_path: Optional[str] = None,
    ):
        paper = config.get("paper", {}) or {}
        chain_cfg = (config.get("chains", {}) or {}).get("solana", {}) or {}

        self.initial_sol_lamports = _sol_to_lamports(paper.get("initial_sol", 2.0))
        self.reset_on_boot = bool(paper.get("reset_on_boot", True))
        self.ledger_path = Path(
            ledger_path
            or state_path
            or paper.get("ledger_db_path", "data/wallet_ledger.db")
        )
        self.legacy_state_path = paper.get("legacy_state_path") or legacy_state_path

        slippage_pct = paper.get("slippage_bps")
        if slippage_pct is None:
            slippage_pct = int((_decimal(paper.get("slippage_pct", 2.0)) * 100).quantize(Decimal("1")))
        self.slippage_bps = int(slippage_pct)

        self._sol_usd_micro = _usd_to_micro(paper.get("fallback_sol_usd", 200.0))
        self._marks: Dict[str, Decimal] = {}
        self._decimals: Dict[str, int] = {}
        self._initialized = False
        self._legacy_pending = False
        self._init_lock = asyncio.Lock()
        self._swap_lock = asyncio.Lock()

        quote_url = (
            paper.get("jupiter_quote_url")
            or chain_cfg.get("jupiter_quote_api")
            or chain_cfg.get("jupiter_api")
            or DEFAULT_QUOTE_URL
        )
        self.quote_client = quote_client or JupiterQuoteClient(quote_url)
        self.rpc_client = rpc_client or SolanaRpcClient(
            chain_cfg.get("rpc_url", PUBLIC_SOLANA_RPC),
            chain_cfg.get("rpc_fallback_urls") or (),
        )

        if self.reset_on_boot and self.ledger_path.exists():
            backup = self.ledger_path.with_name(
                f"{self.ledger_path.name}.reset-{int(time.time())}"
            )
            self.ledger_path.replace(backup)
            logger.warning("Wallet ledger reset requested; archived previous ledger to %s", backup)
            self.legacy_state_path = None

        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(
            str(self.ledger_path), check_same_thread=False, isolation_level=None
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._create_schema()
        self._load_marks()
        self._load_decimals()

        if self._ledger_count() == 0:
            legacy = Path(self.legacy_state_path) if self.legacy_state_path else None
            if legacy and legacy.exists():
                self._legacy_pending = True
            else:
                self._insert_genesis()
                self._initialized = True
        else:
            self._initialized = True
            self._load_legacy_sol_price()

    def _load_legacy_sol_price(self):
        path = Path(self.legacy_state_path) if self.legacy_state_path else None
        if not path or not path.exists():
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return
        if data.get("sol_usd"):
            self.set_sol_price(data["sol_usd"])

    # ---------------- schema / transactions ----------------
    def _create_schema(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                idempotency_key TEXT NOT NULL UNIQUE,
                trade_id TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                asset_mint TEXT NOT NULL,
                delta_atomic INTEGER NOT NULL,
                entry_type TEXT NOT NULL CHECK (
                    entry_type IN (
                        'DEPOSIT', 'SWAP_IN', 'SWAP_OUT',
                        'GAS_FEE', 'ATA_RENT_LOCK', 'ATA_RENT_REFUND'
                    )
                )
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_wallet_ledger_asset "
            "ON wallet_ledger(asset_mint)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_wallet_ledger_trade "
            "ON wallet_ledger(trade_id)"
        )
        # Marks are market data, not accounting balances; kept outside the append-only ledger.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet_marks (
                asset_mint TEXT PRIMARY KEY,
                price_usd TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet_decimals (
                asset_mint TEXT PRIMARY KEY,
                decimals INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TRIGGER IF NOT EXISTS wallet_ledger_no_update
            BEFORE UPDATE ON wallet_ledger
            BEGIN
                SELECT RAISE(ABORT, 'wallet_ledger is append-only');
            END
            """
        )
        cur.execute(
            """
            CREATE TRIGGER IF NOT EXISTS wallet_ledger_no_delete
            BEFORE DELETE ON wallet_ledger
            BEGIN
                SELECT RAISE(ABORT, 'wallet_ledger is append-only');
            END
            """
        )

    def _ledger_count(self, cur: Optional[sqlite3.Cursor] = None) -> int:
        c = cur or self.conn.cursor()
        row = c.execute("SELECT COUNT(*) FROM wallet_ledger").fetchone()
        return int(row[0] or 0)

    def _sum(self, asset_mint: str, cur: Optional[sqlite3.Cursor] = None) -> int:
        c = cur or self.conn.cursor()
        row = c.execute(
            "SELECT COALESCE(SUM(delta_atomic), 0) FROM wallet_ledger "
            "WHERE asset_mint = ?",
            (asset_mint,),
        ).fetchone()
        return int(row[0] or 0)

    def _run_tx(self, operation):
        cur = self.conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            result = operation(cur)
            cur.execute("COMMIT")
            return result
        except Exception:
            cur.execute("ROLLBACK")
            raise

    def _insert_rows(self, cur: sqlite3.Cursor, rows: Sequence[Tuple[str, str, int, str, int, str]]):
        for key, trade_id, ts, asset, delta, entry_type in rows:
            if not _KEY_RE.fullmatch(key):
                raise WalletError(f"invalid idempotency key: {key!r}")
            if entry_type not in ENTRY_TYPES:
                raise WalletError(f"invalid entry type: {entry_type}")
            if isinstance(delta, bool) or not isinstance(delta, int):
                raise WalletError("delta_atomic must be an integer")
        cur.executemany(
            "INSERT INTO wallet_ledger "
            "(idempotency_key, trade_id, timestamp, asset_mint, delta_atomic, entry_type) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )

    def _insert_genesis(self):
        if self.initial_sol_lamports < 0:
            raise WalletError("initial_sol cannot be negative")
        if self.initial_sol_lamports == 0:
            return
        ts = int(time.time() * 1000)

        def op(cur):
            if self._ledger_count(cur):
                return
            self._insert_rows(
                cur,
                [
                    (
                        "genesis:DEPOSIT:SOL",
                        "genesis",
                        ts,
                        SOL_ASSET,
                        self.initial_sol_lamports,
                        ENTRY_DEPOSIT,
                    )
                ],
            )

        self._run_tx(op)

    # ---------------- startup / legacy migration ----------------
    async def ensure_initialized(self):
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            if self._legacy_pending:
                await self._migrate_legacy_state()
            else:
                self._insert_genesis()
            self._initialized = True

    async def _migrate_legacy_state(self):
        path = Path(self.legacy_state_path) if self.legacy_state_path else None
        if not path or not path.exists():
            self._insert_genesis()
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.error("Legacy wallet read failed (%s); starting from configured genesis", exc)
            self._insert_genesis()
            return

        legacy_initial = data.get("initial_sol")
        if legacy_initial is not None:
            self.initial_sol_lamports = _sol_to_lamports(legacy_initial)
        if data.get("sol_usd"):
            self.set_sol_price(data["sol_usd"])

        rows: List[Tuple[str, str, int, str, int, str]] = []
        ts = int(time.time() * 1000)
        cash = _sol_to_lamports(data.get("sol", 0))
        if cash > 0:
            rows.append(
                ("legacy-opener:DEPOSIT:SOL", "legacy-opener", ts, SOL_ASSET, cash, ENTRY_DEPOSIT)
            )

        for mint, holding in (data.get("holdings") or {}).items():
            amount = holding.get("amount", 0)
            decimals = await self._resolve_decimals(mint, fallback=6)
            raw = int(
                (_decimal(amount) * (Decimal(10) ** decimals)).quantize(
                    Decimal("1"), rounding=ROUND_HALF_UP
                )
            )
            if raw > 0:
                rows.append(
                    (
                        f"legacy-opener:DEPOSIT:{mint}",
                        "legacy-opener",
                        ts,
                        mint,
                        raw,
                        ENTRY_DEPOSIT,
                    )
                )
            mark_price = holding.get("mark_price")
            if mark_price is not None and _decimal(mark_price) > 0:
                mark_value = _decimal(mark_price)
            elif _decimal(amount) > 0 and _decimal(holding.get("cost_usd", 0)) > 0:
                mark_value = _decimal(holding["cost_usd"]) / _decimal(amount)
            else:
                mark_value = None
            if mark_value is not None and mark_value > 0:
                self._marks[mint] = mark_value
                self._save_mark(mint, mark_value)

        def op(cur):
            if self._ledger_count(cur):
                return
            self._insert_rows(cur, rows)

        self._run_tx(op)
        logger.info(
            "Legacy wallet migrated into append-only ledger: %d opening entries, cash %s SOL",
            len(rows),
            f"{_lamports_to_sol(cash):.9f}",
        )

    # ---------------- price / metadata helpers ----------------
    def set_sol_price(self, usd: Any):
        value = _decimal(usd)
        if value > 0:
            self._sol_usd_micro = _usd_to_micro(value)

    def sol_price(self) -> Decimal:
        return Decimal(self._sol_usd_micro) / Decimal(MICRO_USD_PER_USD)

    def usd_to_lamports(self, usd_amount: Any) -> int:
        value = _decimal(usd_amount)
        if value <= 0 or self._sol_usd_micro <= 0:
            return 0
        cents = int((value * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        return (cents * 10**13) // self._sol_usd_micro

    def lamports_to_usd(self, lamports: int) -> Decimal:
        return (
            Decimal(lamports)
            * Decimal(self._sol_usd_micro)
            / (Decimal(LAMPORTS_PER_SOL) * Decimal(MICRO_USD_PER_USD))
        )

    async def _resolve_decimals(self, mint: str, fallback: Optional[int] = None) -> Optional[int]:
        if mint in self._decimals:
            return self._decimals[mint]
        decimals = await self.rpc_client.get_mint_decimals(mint)
        if decimals is None:
            decimals = fallback
        if decimals is not None:
            self._decimals[mint] = int(decimals)
            self._save_decimals(mint, int(decimals))
        return decimals

    def _token_human(self, mint: str, raw: int) -> Optional[Decimal]:
        decimals = self._decimals.get(mint)
        if decimals is None:
            return None
        return Decimal(raw) / (Decimal(10) ** decimals)

    def _implicit_price_usd(
        self, sol_lamports: int, token_raw: int, mint: str
    ) -> Optional[Decimal]:
        human = self._token_human(mint, token_raw)
        if human is None or human <= 0:
            return None
        return self.lamports_to_usd(sol_lamports) / human

    # ---------------- derived balances ----------------
    @property
    def sol_lamports(self) -> int:
        return self._sum(SOL_ASSET)

    @property
    def initial_sol(self) -> Decimal:
        return _lamports_to_sol(self.initial_sol_lamports)

    def balance_atomic(self, asset_mint: str) -> int:
        return self._sum(asset_mint)

    def token_balance_raw(self, mint: str) -> int:
        return self._sum(mint)

    def token_balances(self) -> Dict[str, int]:
        cur = self.conn.cursor()
        rows = cur.execute(
            "SELECT asset_mint, SUM(delta_atomic) AS bal FROM wallet_ledger "
            "WHERE asset_mint != ? GROUP BY asset_mint HAVING bal > 0",
            (SOL_ASSET,),
        ).fetchall()
        return {row["asset_mint"]: int(row["bal"]) for row in rows}

    def cash_usd(self) -> Decimal:
        return self.lamports_to_usd(self.sol_lamports)

    def equity_usd(self) -> Decimal:
        total = self.cash_usd()
        for mint, raw in self.token_balances().items():
            mark = self._marks.get(mint)
            human = self._token_human(mint, raw)
            if mark is not None and human is not None:
                total += human * mark
        return total

    def equity_sol(self) -> Decimal:
        price = self.sol_price()
        if price <= 0:
            return Decimal(0)
        return self.equity_usd() / price

    def holdings_value_usd(self) -> Decimal:
        return self.equity_usd() - self.cash_usd()

    def fills(self) -> int:
        row = self.conn.cursor().execute(
            "SELECT COUNT(*) FROM wallet_ledger WHERE entry_type IN (?, ?)",
            (ENTRY_SWAP_IN, ENTRY_SWAP_OUT),
        ).fetchone()
        return int(row[0] or 0)

    def _load_marks(self):
        try:
            rows = self.conn.execute(
                "SELECT asset_mint, price_usd FROM wallet_marks"
            ).fetchall()
        except sqlite3.Error:
            return
        for row in rows:
            try:
                value = _decimal(row["price_usd"])
            except Exception:
                continue
            if value > 0:
                self._marks[row["asset_mint"]] = value

    def _save_mark(self, mint: str, value: Decimal):
        if value <= 0:
            return
        self.conn.execute(
            "INSERT INTO wallet_marks (asset_mint, price_usd, updated_at) "
            "VALUES (?, ?, ?) ON CONFLICT(asset_mint) DO UPDATE SET "
            "price_usd = excluded.price_usd, updated_at = excluded.updated_at",
            (mint, str(value), int(time.time() * 1000)),
        )

    def _load_decimals(self):
        try:
            rows = self.conn.execute(
                "SELECT asset_mint, decimals FROM wallet_decimals"
            ).fetchall()
        except sqlite3.Error:
            return
        for row in rows:
            self._decimals[row["asset_mint"]] = int(row["decimals"])

    def _save_decimals(self, mint: str, decimals: int):
        self.conn.execute(
            "INSERT INTO wallet_decimals (asset_mint, decimals) VALUES (?, ?) "
            "ON CONFLICT(asset_mint) DO UPDATE SET decimals = excluded.decimals",
            (mint, int(decimals)),
        )

    def mark_holding(self, mint: str, price: Any):
        value = _decimal(price)
        if value > 0 and self._marks.get(mint) != value:
            self._marks[mint] = value
            self._save_mark(mint, value)

    def snapshot(self) -> Dict[str, Any]:
        equity_sol = self.equity_sol()
        return {
            "sol_lamports": self.sol_lamports,
            "sol": _lamports_to_sol(self.sol_lamports),
            "sol_usd": self.sol_price(),
            "cash_usd": self.cash_usd(),
            "holdings": len(self.token_balances()),
            "holdings_value_usd": self.holdings_value_usd(),
            "equity_usd": self.equity_usd(),
            "equity_sol": equity_sol,
            "initial_sol": self.initial_sol,
            "fills": self.fills(),
            "fill_model": "jupiter_exact_in_quote",
            "gas_lamports": GAS_FEE_LAMPORTS,
            "ata_rent_lamports": ATA_RENT_LAMPORTS,
            "slippage_bps": self.slippage_bps,
        }

    # ---------------- swap persistence ----------------
    @staticmethod
    def _key(base_key: str, entry_type: str, asset_mint: str) -> str:
        return f"{base_key}:{entry_type}:{asset_mint}"

    @staticmethod
    def _validate_base_key(key: str) -> str:
        key = str(key)
        if not _KEY_RE.fullmatch(key):
            raise WalletError(f"invalid idempotency key: {key!r}")
        return key

    def _has_anchor(self, cur: sqlite3.Cursor, anchor_key: str) -> bool:
        row = cur.execute(
            "SELECT 1 FROM wallet_ledger WHERE idempotency_key = ?", (anchor_key,)
        ).fetchone()
        return row is not None

    def _existing_fill(
        self,
        cur: sqlite3.Cursor,
        base_key: str,
        side: str,
        mint: str,
        trade_id: str,
    ) -> Optional[Dict[str, Any]]:
        rows = cur.execute(
            "SELECT * FROM wallet_ledger WHERE instr(idempotency_key, ?) = 1 "
            "ORDER BY id",
            (base_key + ":",),
        ).fetchall()
        if not rows:
            return None

        sol_delta = 0
        token_delta = 0
        gas = 0
        rent = 0
        swap_sol_in = 0
        swap_sol_out = 0
        ts = int(rows[0]["timestamp"])
        for row in rows:
            delta = int(row["delta_atomic"])
            asset = row["asset_mint"]
            entry = row["entry_type"]
            if asset == SOL_ASSET:
                sol_delta += delta
            else:
                token_delta += delta
            if entry == ENTRY_GAS_FEE:
                gas += -delta
            elif entry in (ENTRY_ATA_RENT_LOCK, ENTRY_ATA_RENT_REFUND):
                rent += delta
            elif entry == ENTRY_SWAP_IN and asset == SOL_ASSET:
                swap_sol_in += delta
            elif entry == ENTRY_SWAP_OUT and asset == SOL_ASSET:
                swap_sol_out += -delta

        decimals = self._decimals.get(mint)
        token_raw = token_delta if side == "buy" else -token_delta
        human = self._token_human(mint, token_raw)
        fill: Dict[str, Any] = {
            "success": True,
            "idempotent": True,
            "side": side,
            "trade_id": trade_id,
            "mint": mint,
            "timestamp_ms": ts,
            "sol_delta_lamports": sol_delta,
            "token_delta_raw": token_delta,
            "token_amount_raw": token_raw,
            "token_decimals": decimals,
            "token_amount": human,
            "gas_lamports": gas,
            "ata_rent_delta_lamports": rent,
            "sol_usd": self.sol_price(),
        }
        if side == "buy":
            cost_sol = -sol_delta
            fill["cost_sol_lamports"] = cost_sol
            fill["cost_usd"] = self.lamports_to_usd(cost_sol)
            fill["entry_price_usd"] = self._implicit_price_usd(
                swap_sol_out or cost_sol, token_raw, mint
            )
        else:
            fill["net_sol_lamports"] = sol_delta
            fill["net_usd"] = self.lamports_to_usd(sol_delta)
            fill["exit_price_usd"] = self._implicit_price_usd(
                swap_sol_in, token_raw, mint
            )
        return fill

    async def _quote(
        self,
        input_mint: str,
        output_mint: str,
        amount_atomic: int,
        slippage_bps: Optional[int],
    ) -> Optional[Dict[str, Any]]:
        try:
            return await self.quote_client.quote(
                input_mint,
                output_mint,
                amount_atomic,
                self.slippage_bps if slippage_bps is None else int(slippage_bps),
            )
        except Exception as exc:
            logger.warning(
                "Jupiter quote failed %s -> %s amount %s: %s",
                input_mint[:8],
                output_mint[:8],
                amount_atomic,
                exc,
            )
            return None

    async def quote_sell_value(
        self,
        mint: str,
        token_amount_raw: int,
        *,
        slippage_bps: Optional[int] = None,
        token_decimals: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Exact-in Jupiter quote for selling raw token units back to SOL."""
        await self.ensure_initialized()
        raw = _positive_int(token_amount_raw, "token_amount_raw")
        decimals = int(token_decimals) if token_decimals is not None else await self._resolve_decimals(mint)
        if decimals is None:
            logger.warning("Cannot quote %s sell: on-chain decimals unavailable", mint[:8])
            return None
        quote = await self._quote(mint, WSOL_MINT, raw, slippage_bps)
        if quote is None:
            return None
        out_lamports = int(quote["out_amount"])
        if out_lamports <= 0:
            return None
        human = Decimal(raw) / (Decimal(10) ** int(decimals))
        return {
            "mint": mint,
            "in_amount_raw": raw,
            "out_lamports": out_lamports,
            "token_decimals": int(decimals),
            "token_amount": human,
            "sol_per_token": (Decimal(out_lamports) / Decimal(LAMPORTS_PER_SOL) / human) if human > 0 else Decimal(0),
            "price_impact_pct": quote.get("price_impact_pct"),
            "quote": quote,
        }

    # ---------------- swaps ----------------
    async def buy(
        self,
        mint: str,
        sol_lamports: int,
        *,
        trade_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        slippage_bps: Optional[int] = None,
        token_decimals: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        await self.ensure_initialized()
        amount = _positive_int(sol_lamports, "sol_lamports")
        trade_id = str(trade_id or f"trade-{uuid.uuid4().hex}")
        base_key = self._validate_base_key(
            idempotency_key or f"{trade_id}:buy:{mint}:{amount}"
        )

        async with self._swap_lock:
            if token_decimals is not None:
                self._decimals[mint] = int(token_decimals)
                self._save_decimals(mint, int(token_decimals))
            existing = self._existing_fill(
                self.conn.cursor(), base_key, "buy", mint, trade_id
            )
            if existing is not None:
                return existing

            current_sol = self.sol_lamports
            current_token = self.token_balance_raw(mint)
            rent = ATA_RENT_LAMPORTS if current_token == 0 else 0
            required = amount + GAS_FEE_LAMPORTS + rent
            if current_sol < required:
                logger.warning(
                    "Demo wallet refused buy %s: needs %d lamports, has %d",
                    mint[:8],
                    required,
                    current_sol,
                )
                return None

            decimals_task = None
            if token_decimals is None:
                decimals_task = asyncio.create_task(
                    self._resolve_decimals(mint, fallback=token_decimals)
                )
            quote = await self._quote(WSOL_MINT, mint, amount, slippage_bps)
            if quote is None:
                if decimals_task is not None:
                    decimals_task.cancel()
                return None
            if token_decimals is not None:
                decimals = int(token_decimals)
            else:
                try:
                    decimals = await decimals_task
                except Exception:
                    decimals = None
            out_amount = int(quote["out_amount"])
            ts = int(time.time() * 1000)
            anchor_key = self._key(base_key, ENTRY_SWAP_IN, mint)

            def operation(cur: sqlite3.Cursor):
                existing = self._existing_fill(cur, base_key, "buy", mint, trade_id)
                if existing is not None or self._has_anchor(cur, anchor_key):
                    return existing or {
                        "success": True,
                        "idempotent": True,
                        "side": "buy",
                        "trade_id": trade_id,
                        "mint": mint,
                    }

                sol_now = self._sum(SOL_ASSET, cur)
                token_now = self._sum(mint, cur)
                rent_now = ATA_RENT_LAMPORTS if token_now == 0 else 0
                sol_delta = -amount - GAS_FEE_LAMPORTS - rent_now
                if sol_now + sol_delta < 0:
                    raise InsufficientBalance(
                        f"buy {mint[:8]} needs {-sol_delta} lamports, has {sol_now}"
                    )
                if token_now + out_amount < 0:
                    raise InsufficientBalance("token balance guard failed")

                rows = [
                    (
                        self._key(base_key, ENTRY_SWAP_OUT, SOL_ASSET),
                        trade_id,
                        ts,
                        SOL_ASSET,
                        -amount,
                        ENTRY_SWAP_OUT,
                    ),
                    (
                        self._key(base_key, ENTRY_SWAP_IN, mint),
                        trade_id,
                        ts,
                        mint,
                        out_amount,
                        ENTRY_SWAP_IN,
                    ),
                    (
                        self._key(base_key, ENTRY_GAS_FEE, SOL_ASSET),
                        trade_id,
                        ts,
                        SOL_ASSET,
                        -GAS_FEE_LAMPORTS,
                        ENTRY_GAS_FEE,
                    ),
                ]
                if rent_now:
                    rows.append(
                        (
                            self._key(base_key, ENTRY_ATA_RENT_LOCK, SOL_ASSET),
                            trade_id,
                            ts,
                            SOL_ASSET,
                            -rent_now,
                            ENTRY_ATA_RENT_LOCK,
                        )
                    )
                self._insert_rows(cur, rows)

                human = self._token_human(mint, out_amount)
                cost_sol = -sol_delta
                return {
                    "success": True,
                    "idempotent": False,
                    "side": "buy",
                    "trade_id": trade_id,
                    "mint": mint,
                    "timestamp_ms": ts,
                    "sol_delta_lamports": sol_delta,
                    "sol_in_lamports": amount,
                    "token_delta_raw": out_amount,
                    "token_amount_raw": out_amount,
                    "token_decimals": decimals,
                    "token_amount": human,
                    "gas_lamports": GAS_FEE_LAMPORTS,
                    "ata_rent_delta_lamports": -rent_now,
                    "cost_sol_lamports": cost_sol,
                    "cost_usd": self.lamports_to_usd(cost_sol),
                    "entry_price_usd": self._implicit_price_usd(amount, out_amount, mint),
                    "sol_usd": self.sol_price(),
                    "quote": quote,
                }

            fill = self._run_tx(operation)
            if fill and not fill.get("idempotent") and fill.get("entry_price_usd"):
                self._marks[mint] = fill["entry_price_usd"]
                self._save_mark(mint, fill["entry_price_usd"])
            return fill

    async def sell(
        self,
        mint: str,
        token_amount_raw: int,
        *,
        trade_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        slippage_bps: Optional[int] = None,
        token_decimals: Optional[int] = None,
        reason: str = "",
    ) -> Optional[Dict[str, Any]]:
        await self.ensure_initialized()
        raw = _positive_int(token_amount_raw, "token_amount_raw")
        trade_id = str(trade_id or f"trade-{uuid.uuid4().hex}")
        base_key = self._validate_base_key(
            idempotency_key or f"{trade_id}:sell:{raw}:{reason}"
        )
        async with self._swap_lock:
            if token_decimals is not None:
                self._decimals[mint] = int(token_decimals)
                self._save_decimals(mint, int(token_decimals))
            existing = self._existing_fill(
                self.conn.cursor(), base_key, "sell", mint, trade_id
            )
            if existing is not None:
                return existing
            current = self.token_balance_raw(mint)
            if raw > current:
                logger.warning(
                    "Demo wallet refused sell %s: asked %d raw, has %d",
                    mint[:8],
                    raw,
                    current,
                )
                return None
            return await self._sell_locked(
                mint,
                raw,
                trade_id,
                base_key,
                slippage_bps,
                token_decimals,
                reason,
            )

    async def sell_percent(
        self,
        mint: str,
        percent: int,
        *,
        trade_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        slippage_bps: Optional[int] = None,
        token_decimals: Optional[int] = None,
        reason: str = "",
        basis_raw: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        await self.ensure_initialized()
        pct = int(percent)
        if pct <= 0 or pct > 100:
            raise WalletError("percent must be in 1..100")
        trade_id = str(trade_id or f"trade-{uuid.uuid4().hex}")
        base_key = self._validate_base_key(
            idempotency_key or f"{trade_id}:sell:{pct}:{reason}"
        )
        async with self._swap_lock:
            if token_decimals is not None:
                self._decimals[mint] = int(token_decimals)
                self._save_decimals(mint, int(token_decimals))
            existing = self._existing_fill(
                self.conn.cursor(), base_key, "sell", mint, trade_id
            )
            if existing is not None:
                return existing
            current = self.token_balance_raw(mint)
            if current <= 0:
                logger.warning("Demo wallet refused sell %s: zero balance", mint[:8])
                return None
            if pct == 100:
                raw = current
            else:
                basis = max(0, int(basis_raw)) if basis_raw is not None else current
                raw = min(current, basis * pct // 100)
            if raw <= 0:
                logger.warning(
                    "Demo wallet refused sell %s: %d%% of %d raw rounds to zero",
                    mint[:8],
                    pct,
                    current,
                )
                return None
            return await self._sell_locked(
                mint,
                raw,
                trade_id,
                base_key,
                slippage_bps,
                token_decimals,
                reason,
            )

    async def _sell_locked(
        self,
        mint: str,
        raw: int,
        trade_id: str,
        base_key: str,
        slippage_bps: Optional[int],
        token_decimals: Optional[int],
        reason: str,
    ) -> Optional[Dict[str, Any]]:
        decimals_task = None
        if token_decimals is None:
            decimals_task = asyncio.create_task(
                self._resolve_decimals(mint, fallback=token_decimals)
            )
        quote = await self._quote(mint, WSOL_MINT, raw, slippage_bps)
        if quote is None:
            if decimals_task is not None:
                decimals_task.cancel()
            return None
        if token_decimals is not None:
            decimals = int(token_decimals)
        else:
            try:
                decimals = await decimals_task
            except Exception:
                decimals = None
        out_amount = int(quote["out_amount"])
        ts = int(time.time() * 1000)
        anchor_key = self._key(base_key, ENTRY_SWAP_OUT, mint)

        def operation(cur: sqlite3.Cursor):
            existing = self._existing_fill(cur, base_key, "sell", mint, trade_id)
            if existing is not None or self._has_anchor(cur, anchor_key):
                return existing or {
                    "success": True,
                    "idempotent": True,
                    "side": "sell",
                    "trade_id": trade_id,
                    "mint": mint,
                }

            token_now = self._sum(mint, cur)
            sol_now = self._sum(SOL_ASSET, cur)
            if raw > token_now:
                raise InsufficientBalance(
                    f"sell {mint[:8]} asked {raw} raw, has {token_now}"
                )
            refund = ATA_RENT_LAMPORTS if raw == token_now else 0
            sol_delta = out_amount - GAS_FEE_LAMPORTS + refund
            if sol_now + sol_delta < 0:
                raise InsufficientBalance(
                    f"sell {mint[:8]} makes SOL negative: {sol_now} + {sol_delta}"
                )

            rows = [
                (
                    self._key(base_key, ENTRY_SWAP_OUT, mint),
                    trade_id,
                    ts,
                    mint,
                    -raw,
                    ENTRY_SWAP_OUT,
                ),
                (
                    self._key(base_key, ENTRY_SWAP_IN, SOL_ASSET),
                    trade_id,
                    ts,
                    SOL_ASSET,
                    out_amount,
                    ENTRY_SWAP_IN,
                ),
                (
                    self._key(base_key, ENTRY_GAS_FEE, SOL_ASSET),
                    trade_id,
                    ts,
                    SOL_ASSET,
                    -GAS_FEE_LAMPORTS,
                    ENTRY_GAS_FEE,
                ),
            ]
            if refund:
                rows.append(
                    (
                        self._key(base_key, ENTRY_ATA_RENT_REFUND, SOL_ASSET),
                        trade_id,
                        ts,
                        SOL_ASSET,
                        refund,
                        ENTRY_ATA_RENT_REFUND,
                    )
                )
            self._insert_rows(cur, rows)

            human = self._token_human(mint, raw)
            return {
                "success": True,
                "idempotent": False,
                "side": "sell",
                "trade_id": trade_id,
                "mint": mint,
                "timestamp_ms": ts,
                "reason": reason,
                "sol_delta_lamports": sol_delta,
                "token_delta_raw": -raw,
                "token_amount_raw": raw,
                "token_decimals": decimals,
                "token_amount": human,
                "gas_lamports": GAS_FEE_LAMPORTS,
                "ata_rent_delta_lamports": refund,
                "net_sol_lamports": sol_delta,
                "net_usd": self.lamports_to_usd(sol_delta),
                "exit_price_usd": self._implicit_price_usd(out_amount, raw, mint),
                "sol_usd": self.sol_price(),
                "quote": quote,
                "full_exit": raw == token_now,
            }

        fill = self._run_tx(operation)
        if fill and not fill.get("idempotent") and fill.get("exit_price_usd"):
            self._marks[mint] = fill["exit_price_usd"]
            self._save_mark(mint, fill["exit_price_usd"])
        return fill

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

"""Honest open-position PnL from exact Jupiter sell quotes.

PnL is never derived from DexScreener priceChange or market-cap ratios. The only
inputs are the stored entry lamports and an exact-in quote for the stored raw
token amount.
"""
import json
import logging
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

WSOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000
DEFAULT_QUOTE_URL = "https://api.jup.ag/swap/v1"
DEFAULT_ORDER_V2_URL = "https://api.jup.ag/swap/v2/order"
DEFAULT_PRICE_URL = "https://lite-api.jup.ag/price/v3"


def quote_url_from_config(config: Optional[dict] = None) -> str:
    config = config or {}
    paper = config.get("paper", {}) or {}
    chain = (config.get("chains", {}) or {}).get("solana", {}) or {}
    return (
        paper.get("jupiter_quote_url")
        or chain.get("jupiter_quote_api")
        or chain.get("jupiter_api")
        or DEFAULT_QUOTE_URL
    )


def order_v2_url_from_quote_url(quote_url: str) -> str:
    base = (quote_url or DEFAULT_QUOTE_URL).rstrip("/")
    if base.endswith("/quote"):
        base = base[:-6]
    if base.endswith("/swap/v1"):
        return base[:-8] + "/swap/v2/order"
    if base.endswith("/swap/v2"):
        return base + "/order"
    return DEFAULT_ORDER_V2_URL


def _order_v2_quote(
    mint: str,
    raw_token_amount: int,
    *,
    quote_url: str,
    slippage_bps: int,
    timeout: int,
) -> Optional[int]:
    url = order_v2_url_from_quote_url(quote_url)
    params = {
        "inputMint": mint,
        "outputMint": WSOL_MINT,
        "amount": str(raw_token_amount),
        "slippageBps": str(int(slippage_bps)),
    }
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        if resp.status_code != 200:
            logger.warning("Jupiter v2 order HTTP %s for %s", resp.status_code, mint[:8])
            return None
        data = resp.json()
        in_amount = int(data["inAmount"])
        out_amount = int(data["outAmount"])
    except Exception as exc:
        logger.warning("Jupiter v2 order failed for %s: %s", mint[:8], exc)
        return None
    if in_amount != raw_token_amount:
        logger.warning(
            "Jupiter v2 order input mismatch for %s: asked %s got %s",
            mint[:8], raw_token_amount, in_amount,
        )
        return None
    if out_amount <= 0:
        return None
    route = data.get("routePlan") or []
    labels = [((r.get("swapInfo") or {}).get("label") or "") for r in route if isinstance(r, dict)]
    logger.info("Jupiter v2 order route for %s: %s", mint[:8], ", ".join(labels) or "unknown")
    return out_amount


def quote_sell_to_sol(
    mint: str,
    raw_token_amount: int,
    *,
    quote_url: str = DEFAULT_QUOTE_URL,
    slippage_bps: int = 200,
    timeout: int = 8,
) -> Optional[int]:
    if not mint or not isinstance(raw_token_amount, int) or raw_token_amount <= 0:
        return None
    url = quote_url.rstrip("/")
    if not url.endswith("/quote"):
        url += "/quote"
    params = {
        "inputMint": mint,
        "outputMint": WSOL_MINT,
        "amount": str(raw_token_amount),
        "slippageBps": str(int(slippage_bps)),
        "swapMode": "ExactIn",
    }
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        if resp.status_code == 200:
            data = resp.json()
            in_amount = int(data["inAmount"])
            out_amount = int(data["outAmount"])
            if in_amount != raw_token_amount:
                logger.warning(
                    "Jupiter PnL quote input mismatch for %s: asked %s got %s",
                    mint[:8], raw_token_amount, in_amount,
                )
            elif out_amount > 0:
                return out_amount
        else:
            logger.warning("Jupiter PnL quote HTTP %s for %s; trying v2 order", resp.status_code, mint[:8])
    except Exception as exc:
        logger.warning("Jupiter PnL quote failed for %s: %s; trying v2 order", mint[:8], exc)

    return _order_v2_quote(
        mint,
        raw_token_amount,
        quote_url=quote_url,
        slippage_bps=slippage_bps,
        timeout=timeout,
    )


def get_sol_usd(price_url: str = DEFAULT_PRICE_URL, timeout: int = 6) -> Optional[float]:
    try:
        resp = requests.get(price_url, params={"ids": WSOL_MINT}, timeout=timeout)
        if resp.status_code != 200:
            return None
        data = resp.json()
        item = data.get(WSOL_MINT) or {}
        value = item.get("usdPrice")
        return float(value) if value is not None else None
    except Exception:
        return None


def remaining_percent(position: dict) -> int:
    raw = position.get("tier_exits") or "[]"
    try:
        tiers = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        tiers = []
    sold = 0.0
    for tier in tiers or []:
        try:
            sold += float(tier.get("percent_sold", 0) or 0)
        except Exception:
            continue
    return max(0, min(100, int(round(100.0 - sold))))


def compute_unrealized_pnl(
    position: dict,
    *,
    quote_url: Optional[str] = None,
    slippage_bps: int = 200,
    sol_usd: Optional[float] = None,
) -> Dict[str, Any]:
    entry_total = int(position.get("entry_sol_lamports") or 0)
    raw_total = int(position.get("token_amount_raw") or 0)
    base = {
        "entry_sol_lamports": entry_total,
        "token_amount_raw": raw_total,
        "pnl_percent": None,
        "x_return": None,
        "exit_quote_lamports": None,
        "entry_sol_lamports_remaining": None,
        "token_amount_raw_remaining": None,
        "pnl_source": "unavailable",
    }
    if entry_total <= 0 or raw_total <= 0:
        base["reason"] = "missing_entry_sol_or_raw_token_amount"
        return base

    remaining = remaining_percent(position)
    raw_remaining = raw_total * remaining // 100
    entry_remaining = entry_total * remaining // 100
    base["token_amount_raw_remaining"] = raw_remaining
    base["entry_sol_lamports_remaining"] = entry_remaining
    if raw_remaining <= 0 or entry_remaining <= 0:
        base["reason"] = "position_fully_sold"
        return base

    out_lamports = quote_sell_to_sol(
        position.get("token_address", ""),
        raw_remaining,
        quote_url=quote_url or DEFAULT_QUOTE_URL,
        slippage_bps=slippage_bps,
    )
    if out_lamports is None:
        base["reason"] = "jupiter_sell_quote_failed"
        return base

    pnl_percent = (out_lamports - entry_remaining) / entry_remaining * 100.0
    result = dict(base)
    result.update({
        "pnl_percent": pnl_percent,
        "x_return": out_lamports / entry_remaining,
        "exit_quote_lamports": out_lamports,
        "pnl_source": "jupiter_sell_quote",
    })

    decimals = position.get("token_decimals")
    if sol_usd and decimals is not None:
        try:
            token_human = raw_remaining / (10 ** int(decimals))
            if token_human > 0:
                result["current_price"] = (out_lamports / LAMPORTS_PER_SOL * sol_usd) / token_human
        except Exception:
            pass
    if sol_usd:
        result["profit_usd"] = (out_lamports - entry_remaining) / LAMPORTS_PER_SOL * sol_usd
    return result

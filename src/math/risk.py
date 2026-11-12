"""Risk math ported from C:/quant-bot/quantbot/mathlib/kelly.py + risk.py
Adapted for meme skewed payoffs.
"""
from __future__ import annotations
import numpy as np
import pandas as pd


def kelly_fraction(win_pct: float, payoff_ratio: float, fractional: float = 0.25, cap: float = 0.05) -> float:
    """Kelly fraction f* = fraction * (p*(b+1)-1)/b capped.
    fractional=0.25 = quarter-Kelly (standard for noisy edge, quant-bot uses 0.25 in risk.py:27)
    cap=0.05 = max 5% equity per bet (quant-bot RISK_MAX_POSITION_PCT)
    For memes we default cap 2% via config, caller passes cap.
    """
    p = max(0.0, min(1.0, win_pct))
    b = max(0.1, payoff_ratio)
    f = (p * (b + 1) - 1) / b if b > 0 else 0.0
    f = max(0.0, f)
    return float(np.clip(fractional * f, 0.0, cap))


def meme_kelly_from_trades(pnls: list[float], fractional: float = 0.25, cap: float = 0.02) -> float:
    """Derive Kelly fraction from trade PnL history. Handles meme skew (many -100%, few +500%).
    pnls = list of trade return multiples e.g. +4.0 = 5x (400% profit), -1.0 = rug (-100%)
    """
    if len(pnls) < 5:
        return 0.0
    wins = [x for x in pnls if x > 0]
    losses = [x for x in pnls if x <= 0]
    if not wins or not losses:
        return 0.0
    p = len(wins) / len(pnls)
    avg_win = float(np.mean(wins))
    avg_loss = abs(float(np.mean(losses)))
    if avg_loss == 0:
        return 0.0
    b = avg_win / avg_loss
    return kelly_fraction(p, b, fractional=fractional, cap=cap)


def vol_target_weight(annual_vol: float, target_vol: float = 0.40, max_weight: float = 1.0) -> float:
    """quantbot/mathlib/risk.py:9 vol_target_weight.
    For memes target_vol 0.40 (40% annual is conservative for memes that do 10x in hours).
    If vol is 200% annual, weight = 0.40/2.0 = 0.20 -> size at 20% of Kelly.
    """
    if annual_vol is None or not np.isfinite(annual_vol) or annual_vol <= 0:
        return max_weight
    return float(min(max_weight, target_vol / annual_vol))


def atr_stop(entry_price: float, side: int, atr_value: float, mult: float = 2.5) -> float:
    """quantbot/mathlib/risk.py:22 atr_stop. side +1 long (only longs for memes)."""
    dist = mult * atr_value
    return float(entry_price - dist) if side > 0 else float(entry_price + dist)


def atr_target(entry_price: float, side: int, atr_value: float, mult: float = 3.0) -> float:
    dist = mult * atr_value
    return float(entry_price + dist) if side > 0 else float(entry_price - dist)


def halt_on_drawdown(equity: pd.Series, max_dd_pct: float = 8.0) -> bool:
    """quantbot/mathlib/risk.py:42 - hard kill switch."""
    if len(equity) < 2:
        return False
    peak = equity.cummax().iloc[-1]
    dd = (equity.iloc[-1] / peak - 1.0) * 100.0
    return dd <= -abs(max_dd_pct)


def compute_atr(prices: list[float], period: int = 14) -> float:
    """ATR for memes: use close-to-close as proxy (no OHLC for new tokens)."""
    if len(prices) < period + 1:
        if len(prices) < 2:
            return 0.0
        rets = [abs(prices[i] - prices[i-1]) / max(prices[i-1], 1e-12) for i in range(1, len(prices))]
        return float(np.mean(rets) * prices[-1]) if rets else 0.0
    # Wilder's ATR on close diffs
    trs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    atr = float(np.mean(trs[:period]))
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def position_size(equity: float, kelly_frac: float, vol_weight: float,
                  entry_price: float, atr: float, atr_stop_mult: float = 1.8) -> dict:
    """Combine Kelly + vol targeting + ATR stop into qty.
    Mirrors quantbot/mathlib/kelly.py:32 position_size_from_kelly
    """
    effective = kelly_frac * vol_weight
    # Dollar risk
    risk_dollar = equity * effective
    stop_dist = atr * atr_stop_mult if atr and atr > 0 else entry_price * 0.30  # 30% default stop for memes
    stop_price = entry_price - stop_dist
    qty = risk_dollar / max(stop_dist, entry_price * 0.05)  # floor 5% stop to avoid absurd qty
    # Also cap notional: qty * entry_price <= equity * kelly_frac / vol_weight? simplified to effective
    max_notional = equity * effective * 5  # allow 5x risk dollar as notional cap (memes: small risk = ~2-10% position)
    qty = min(qty, max_notional / max(entry_price, 1e-12))
    return {
        "qty": float(qty),
        "notional_usd": float(qty * entry_price),
        "position_pct": float((qty * entry_price) / max(equity, 1)),
        "stop_price": float(stop_price),
        "risk_dollar": float(risk_dollar),
        "kelly_frac": float(kelly_frac),
        "vol_weight": float(vol_weight),
        "effective_frac": float(effective),
    }

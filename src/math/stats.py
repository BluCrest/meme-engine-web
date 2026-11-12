"""Stats ported from C:/quant-bot/quantbot/mathlib/stats.py"""
from __future__ import annotations
import numpy as np
import pandas as pd

def returns_of(equity: pd.Series) -> pd.Series:
    return equity.pct_change().dropna()

def annualized_vol(ret: pd.Series, ppy: int = 252) -> float:
    r = ret.dropna()
    if len(r) < 2: return 0.0
    return float(r.std() * np.sqrt(ppy))

def sharpe(ret: pd.Series, rf: float = 0.0, ppy: int = 252) -> float:
    r = ret.dropna()
    if len(r) < 2: return 0.0
    excess = r - rf / ppy
    sd = r.std()
    if sd == 0: return 0.0
    return float(np.sqrt(ppy) * excess.mean() / sd)

def sortino(ret: pd.Series, rf: float = 0.0, ppy: int = 252) -> float:
    r = ret.dropna()
    if len(r) < 2: return 0.0
    excess = r - rf / ppy
    downside = r[r < 0]
    if len(downside)==0: return 0.0
    dd = np.sqrt((downside**2).mean())
    if dd==0: return 0.0
    return float(np.sqrt(ppy) * excess.mean() / dd)

def max_drawdown(equity: pd.Series):
    peak = equity.cummax()
    dd = equity / peak - 1.0
    return float(dd.min()), dd.idxmin()

def cagr(equity: pd.Series, ppy: int = 252) -> float:
    if len(equity) < 2: return 0.0
    total = float(equity.iloc[-1] / equity.iloc[0])
    if total <= 0: return -1.0
    return float(total ** (ppy/len(equity)) - 1.0)

def calmar(equity: pd.Series) -> float:
    dd,_ = max_drawdown(equity)
    if dd == 0: return 0.0
    return cagr(equity) / abs(dd)

def win_rate_and_payoff(pnls: list[float]) -> tuple[float,float]:
    if not pnls: return 0.0, 0.0
    wins = [x for x in pnls if x>0]
    losses = [abs(x) for x in pnls if x<=0]
    wr = len(wins)/len(pnls) if pnls else 0
    payoff = (sum(wins)/len(wins))/(sum(losses)/len(losses)) if wins and losses and sum(losses)!=0 else 0
    return wr, payoff

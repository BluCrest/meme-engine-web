"""Demo (dry-run) wallet for the meme engine - Solana only.

Design rules (director order 2026-11-11):
- NO hidden costs: every fill applies slippage + fee BEFORE any return math.
- x (return multiple) is ALL-IN: x = total net proceeds USD / total cost USD.
- ROI% = (x - 1) * 100. pnl_usd = proceeds - cost.
- State persists to data/demo_wallet.json so restarts never lose the book.
- Nothing here touches real keys or network sends. Simulation only.
"""
from __future__ import annotations

import json
import logging
import time
import threading
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class DemoWallet:
    """Paper wallet: cash in SOL, holdings per mint, honest fills, full ledger."""

    def __init__(self, config: dict, state_path: str = "data/demo_wallet.json"):
        paper = config.get('paper', {}) or {}
        self.initial_sol = float(paper.get('initial_sol', 2.0))
        self.slippage_pct = float(paper.get('slippage_pct', 2.0))
        self.fee_pct = float(paper.get('fee_pct', 0.30))
        self.fallback_sol_usd = float(paper.get('fallback_sol_usd', 200.0))
        self.reset_on_boot = bool(paper.get('reset_on_boot', True))
        self.state_path = Path(state_path)
        self._lock = threading.Lock()

        self.sol: float = 0.0
        self.holdings: Dict[str, dict] = {}   # mint -> {amount, cost_usd, cost_sol}
        self.ledger: list = []                # every fill, with cost breakdown
        self.fills = 0
        self.sol_usd = self.fallback_sol_usd

        if self.reset_on_boot and self.state_path.exists():
            self.state_path.unlink()
            logger.info("Demo wallet RESET requested (paper.reset_on_boot=true) - fresh book")
        self._load()

    # ---------------- persistence ----------------
    def _load(self):
        if self.state_path.exists():
            try:
                d = json.loads(self.state_path.read_text(encoding='utf-8'))
                self.sol = float(d.get('sol', self.initial_sol))
                self.holdings = d.get('holdings', {})
                self.ledger = d.get('ledger', [])
                self.fills = int(d.get('fills', len(self.ledger)))
                self.sol_usd = float(d.get('sol_usd', self.fallback_sol_usd))
                logger.info(f"Demo wallet loaded: {self.sol:.6f} SOL, {len(self.holdings)} holdings, {self.fills} fills")
                return
            except Exception as e:
                logger.error(f"Demo wallet load failed ({e}) - starting fresh")
        self.sol = self.initial_sol
        self.holdings = {}
        self.ledger = []
        self.fills = 0
        self._save()

    def _save(self):
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(json.dumps({
                'sol': self.sol,
                'holdings': self.holdings,
                'ledger': self.ledger[-500:],
                'fills': self.fills,
                'sol_usd': self.sol_usd,
                'initial_sol': self.initial_sol,
                'saved_at': time.time(),
            }, indent=2), encoding='utf-8')
        except Exception as e:
            logger.error(f"Demo wallet save failed: {e}")

    # ---------------- price helpers ----------------
    def set_sol_price(self, usd: float):
        if usd and usd > 0:
            self.sol_usd = float(usd)

    def sol_price(self) -> float:
        return self.sol_usd

    # ---------------- fills ----------------
    def buy(self, mint: str, usd_amount: float, token_price: float) -> Optional[dict]:
        """Buy `token_price` USD/token with `usd_amount` USD of SOL cash.
        Fee taken first, then slippage pushes the exec price UP for a buy.
        Returns fill dict or None if refused."""
        if token_price <= 0 or usd_amount <= 0:
            return None
        with self._lock:
            usd_amount = float(usd_amount)
            sol_in = usd_amount / max(self.sol_usd, 1e-9)
            if sol_in > self.sol + 1e-12:
                logger.warning(f"Demo wallet: refused buy of ${usd_amount:.2f} - only {self.sol:.6f} SOL cash")
                return None
            fee_usd = usd_amount * (self.fee_pct / 100.0)
            exec_price = token_price * (1.0 + self.slippage_pct / 100.0)
            tokens = (usd_amount - fee_usd) / exec_price
            self.sol -= sol_in
            h = self.holdings.setdefault(mint, {'amount': 0.0, 'cost_usd': 0.0, 'cost_sol': 0.0})
            h['amount'] += tokens
            h['cost_usd'] += usd_amount
            h['cost_sol'] += sol_in
            self.fills += 1
            fill = {
                'n': self.fills, 't': time.time(), 'side': 'buy', 'mint': mint,
                'usd_amount': usd_amount, 'sol_in': sol_in, 'fee_usd': fee_usd,
                'mid_price': token_price, 'exec_price': exec_price,
                'slippage_pct': self.slippage_pct, 'fee_pct': self.fee_pct,
                'tokens': tokens, 'sol_usd': self.sol_usd,
            }
            self.ledger.append(fill)
            self._save()
            return fill

    def sell(self, mint: str, token_amount: float, token_price: float,
             reason: str = '') -> Optional[dict]:
        """Sell tokens at `token_price` USD/token.
        Slippage pushes exec price DOWN for a sell, then fee comes off the gross.
        Returns fill dict with net_usd/net_sol, or None."""
        h = self.holdings.get(mint)
        if not h or h['amount'] <= 0 or token_price <= 0 or token_amount <= 0:
            return None
        with self._lock:
            token_amount = min(float(token_amount), h['amount'])
            frac = token_amount / h['amount']
            exec_price = token_price * (1.0 - self.slippage_pct / 100.0)
            gross_usd = token_amount * exec_price
            fee_usd = gross_usd * (self.fee_pct / 100.0)
            net_usd = gross_usd - fee_usd
            net_sol = net_usd / max(self.sol_usd, 1e-9)
            self.sol += net_sol
            h['amount'] -= token_amount
            h['cost_usd'] *= (1.0 - frac)
            h['cost_sol'] *= (1.0 - frac)
            if h['amount'] <= 1e-12:
                self.holdings.pop(mint, None)
            self.fills += 1
            fill = {
                'n': self.fills, 't': time.time(), 'side': 'sell', 'mint': mint,
                'tokens': token_amount, 'mid_price': token_price,
                'exec_price': exec_price, 'slippage_pct': self.slippage_pct,
                'gross_usd': gross_usd, 'fee_usd': fee_usd, 'net_usd': net_usd,
                'net_sol': net_sol, 'sol_usd': self.sol_usd, 'reason': reason,
            }
            self.ledger.append(fill)
            self._save()
            return fill

    # ---------------- honest return math ----------------
    @staticmethod
    def x_return(cost_usd: float, proceeds_usd: float) -> float:
        """ALL-IN x multiple: proceeds/cost. x=1.0 means break-even AFTER
        slippage and fees on both legs. Never lie with mid-price math."""
        if cost_usd <= 0:
            return 0.0
        return proceeds_usd / cost_usd

    @staticmethod
    def roi_pct(cost_usd: float, proceeds_usd: float) -> float:
        return (DemoWallet.x_return(cost_usd, proceeds_usd) - 1.0) * 100.0

    @staticmethod
    def pnl_usd(cost_usd: float, proceeds_usd: float) -> float:
        return proceeds_usd - cost_usd

    # ---------------- reporting ----------------
    def equity_usd(self) -> float:
        """Cash SOL marked to market + holdings marked to their last known price."""
        total = self.sol * self.sol_usd
        for mint, h in self.holdings.items():
            total += h.get('mark_usd', h['cost_usd'])  # caller updates mark via mark_holding()
        return total

    def mark_holding(self, mint: str, price: float):
        """Caller (exit manager) updates the mark price each tick."""
        h = self.holdings.get(mint)
        if h and price > 0:
            h['mark_usd'] = h['amount'] * price
            h['mark_price'] = price

    def snapshot(self) -> dict:
        holdings_val = sum(h.get('mark_usd', 0.0) for h in self.holdings.values())
        return {
            'sol': round(self.sol, 6),
            'sol_usd': self.sol_usd,
            'cash_usd': round(self.sol * self.sol_usd, 2),
            'holdings': len(self.holdings),
            'holdings_value_usd': round(holdings_val, 2),
            'equity_usd': round(self.sol * self.sol_usd + holdings_val, 2),
            'fills': self.fills,
            'fee_pct': self.fee_pct,
            'slippage_pct': self.slippage_pct,
        }

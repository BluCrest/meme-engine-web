"""Mini Transformer - lightweight online pattern learner for Sol.
No PyTorch, no heavy deps. Learns feature weights from recent wins/losses online.
Replaces the heavy transformer with a simple, effective linear scorer that adapts.
"""
from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class MiniTransformer:
    """Online linear model with adaptive feature weights.
    - Features: dict of feature_name -> value (normalized 0-1)
    - Labels: 1 for win, 0 for loss
    - Updates weights via simple gradient step (Perceptron-style) on each closed trade
    - Keeps top-k features by weight magnitude for interpretability
    - No PyTorch, no GPU, trains in microseconds.
    """

    def __init__(self, state_path: str = "data/mini_transformer.json", lr: float = 0.05):
        self.state_path = Path(state_path)
        self.lr = lr
        self.weights: Dict[str, float] = defaultdict(float)  # feature -> weight
        self.bias: float = 0.0
        self.n_updates: int = 0
        self.feature_stats: Dict[str, Dict] = defaultdict(lambda: {'wins': 0, 'losses': 0, 'sum_val': 0.0})
        self._load()

    def _load(self):
        if self.state_path.exists():
            try:
                d = json.loads(self.state_path.read_text(encoding='utf-8'))
                self.weights = defaultdict(float, d.get('weights', {}))
                self.bias = float(d.get('bias', 0.0))
                self.n_updates = int(d.get('n_updates', 0))
                self.feature_stats = defaultdict(lambda: {'wins': 0, 'losses': 0, 'sum_val': 0.0},
                                                 {k: v for k, v in d.get('feature_stats', {}).items()})
                logger.info(f"MiniTransformer loaded: {len(self.weights)} features, {self.n_updates} updates")
            except Exception as e:
                logger.warning(f"MiniTransformer load failed: {e}")

    def _save(self):
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(json.dumps({
                'weights': dict(self.weights),
                'bias': self.bias,
                'n_updates': self.n_updates,
                'feature_stats': {k: v for k, v in self.feature_stats.items()},
                'saved_at': time.time(),
            }, indent=2), encoding='utf-8')
        except Exception as e:
            logger.error(f"MiniTransformer save failed: {e}")

    def predict(self, features: Dict[str, float]) -> float:
        """Return win probability (0-1) via sigmoid of linear score."""
        score = self.bias
        for fname, fval in features.items():
            if fval is not None:
                score += self.weights.get(fname, 0.0) * float(fval)
        # sigmoid
        return 1.0 / (1.0 + pow(2.71828, -score))

    def update(self, features: Dict[str, float], label: int):
        """Single online update: perceptron step toward correct label."""
        pred = self.predict(features)
        target = 1.0 if label > 0 else 0.0
        error = target - pred
        # gradient step on logistic loss
        for fname, fval in features.items():
            if fval is not None:
                self.weights[fname] += self.lr * error * float(fval)
                # track stats
                self.feature_stats[fname]['sum_val'] += float(fval)
                if label > 0:
                    self.feature_stats[fname]['wins'] += 1
                else:
                    self.feature_stats[fname]['losses'] += 1
        self.bias += self.lr * error
        self.n_updates += 1
        if self.n_updates % 50 == 0:
            self._save()

    def get_top_features(self, k: int = 10) -> List[Dict]:
        """Return top k features by |weight| with win/loss stats."""
        items = sorted(self.weights.items(), key=lambda x: abs(x[1]), reverse=True)[:k]
        return [{
            'feature': fname,
            'weight': round(w, 4),
            'wins': self.feature_stats[fname]['wins'],
            'losses': self.feature_stats[fname]['losses'],
            'avg_val': round(self.feature_stats[fname]['sum_val'] / max(1, self.feature_stats[fname]['wins'] + self.feature_stats[fname]['losses']), 4)
        } for fname, w in items]

    def score_setup(self, features: Dict[str, float]) -> float:
        """Alias for predict - returns win probability 0-1."""
        return self.predict(features)

    def reset(self):
        """Full reset (use with caution)."""
        self.weights.clear()
        self.bias = 0.0
        self.n_updates = 0
        self.feature_stats.clear()
        self._save()


class MiniTransformerFeaturizer:
    """Converts raw signal data into normalized features for MiniTransformer."""

    @staticmethod
    def featurize(signal: dict, wallet_equity: float = 0) -> Dict[str, float]:
        """Extract ~15 normalized features from a signal."""
        feats = {}

        # Price action
        price = signal.get('price', 0) or 0
        mcap = signal.get('mcap', 0) or 0
        liq = signal.get('liquidity_usd', 0) or 0
        vol24h = signal.get('volume24h', 0) or 0

        # Normalize (log scale for skewed distributions)
        import math
        feats['log_mcap'] = min(max((math.log10(max(mcap, 1)) - 3) / 4, 0), 1)  # 1k-100M
        feats['log_liq'] = min(max((math.log10(max(liq, 1)) - 2) / 4, 0), 1)    # 100-1M
        feats['log_vol'] = min(max((math.log10(max(vol24h, 1)) - 2) / 4, 0), 1) # 100-1M

        # Price level (very small = early)
        feats['price_level'] = min(max(-math.log10(max(price, 1e-12)) / 10, 0), 1)  # 1e-10 to 1

        # Score from scout/overseer
        score = signal.get('score', 50) or 50
        feats['score_norm'] = score / 100.0

        # Time features
        import datetime as dt
        now = dt.datetime.now(dt.timezone.utc)
        hour = now.hour
        feats['hour_sin'] = math.sin(2 * math.pi * hour / 24)
        feats['hour_cos'] = math.cos(2 * math.pi * hour / 24)
        feats['is_us_session'] = 1.0 if 13 <= hour <= 21 else 0.0  # 8am-4pm ET

        # Chain
        chain = signal.get('chain', 'solana')
        feats['is_solana'] = 1.0 if chain == 'solana' else 0.0

        # Source type
        source = signal.get('source', '')
        feats['is_graduation'] = 1.0 if 'grad' in source.lower() or 'migration' in source.lower() else 0.0
        feats['is_new_pair'] = 1.0 if 'new' in source.lower() or 'pair' in source.lower() else 0.0

        # Wallet equity context
        feats['equity_level'] = min(max(wallet_equity / 1000.0, 0), 1)  # cap at 1k

        # Volatility proxy (mcap/liq ratio)
        if liq > 0:
            feats['mcap_liq_ratio'] = min(max(mcap / liq / 10.0, 0), 1)  # cap at 10x
        else:
            feats['mcap_liq_ratio'] = 0.5

        return feats
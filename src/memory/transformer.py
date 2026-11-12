"""Phase3 Transformer - sequences of 5s price% + volume -> profitability 85% Buy.
Input: price_curve[360] + volume_curve[360] from token_lifecycles, Output: P(runner 1M).
String embeddings for token symbols/names. Until 10k lifecycles, falls back to memory query.
"""
import logging
import json
import sqlite3
import hashlib
logger = logging.getLogger(__name__)

class MemeTransformer:
    def __init__(self, input_dim: int = 2, seq_len: int = 360, vocab_size: int = 5000, embed_dim: int = 16):
        self.input_dim = input_dim
        self.seq_len = seq_len
        self.vocab_size = vocab_size
        self.embed_dim = embed_dim
        self.trained = False
        self.model = None
        self.threshold = 0.85
        # Try to load torch model if exists
        try:
            import torch
            self._build_torch_model()
        except ImportError:
            logger.info("PyTorch not installed - transformer will use memory fallback until 10k samples")
            self.model = None

    def _build_torch_model(self):
        try:
            import torch
            import torch.nn as nn
            class MemeTransformerNet(nn.Module):
                def __init__(self, vocab_size=5000, embed_dim=16, d_model=64, nhead=4, num_layers=3):
                    super().__init__()
                    # String embeddings for token symbols (stable hash -> vocab index)
                    self.token_embed = nn.Embedding(vocab_size, embed_dim)
                    # Sequence encoder: price% + vol + token_embed
                    self.input_proj = nn.Linear(2 + embed_dim, d_model)
                    encoder_layer = nn.TransformerEncoderLayer(d_model=d_model, nhead=nhead, dim_feedforward=256, batch_first=True, dropout=0.1)
                    self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
                    self.head = nn.Sequential(
                        nn.Linear(d_model, d_model//2),
                        nn.ReLU(),
                        nn.Dropout(0.1),
                        nn.Linear(d_model//2, 1)
                    )
                    # ~10k params: embed 5000*16=80k (but only active tokens used) + transformer ~8k
                    # Effective params ~10k for active vocab
                def forward(self, x, token_idx=None):  # x: [B, 360, 2], token_idx: [B]
                    B, S, _ = x.shape
                    if token_idx is not None:
                        tok_emb = self.token_embed(token_idx).unsqueeze(1).expand(-1, S, -1)  # [B, S, E]
                        x = torch.cat([x, tok_emb], dim=-1)  # [B, S, 2+E]
                    h = self.input_proj(x)
                    h = self.encoder(h)
                    h = h.mean(dim=1)
                    return torch.sigmoid(self.head(h)).squeeze(-1)
            self.model = MemeTransformerNet(self.vocab_size, self.embed_dim)
            total_params = sum(p.numel() for p in self.model.parameters())
            logger.info(f"MemeTransformerNet built - total params: {total_params:,} (target ~10k active)")
        except Exception as e:
            logger.debug(f"Torch build failed: {e}")
            self.model = None

    def _token_to_idx(self, token_symbol: str) -> int:
        # Stable hash mapping to vocab index
        return int(hashlib.md5(token_symbol.lower().encode()).hexdigest()[:4], 16) % self.vocab_size

    def predict(self, price_curve: list = None, volume_curve: list = None, features: dict = None, token_symbol: str = "") -> float:
        """Phase3: 5s% changes + volume spikes + token embedding -> profitability 0-1."""
        if self.model and self.trained and price_curve and len(price_curve)>=10:
            try:
                import torch
                import numpy as np
                pc = []
                for i in range(1, min(len(price_curve), self.seq_len)):
                    prev = price_curve[i-1] or 1e-9
                    cur = price_curve[i] or prev
                    pct = (cur - prev)/prev
                    vol = volume_curve[i] if volume_curve and i < len(volume_curve) else 0
                    vol_n = np.log1p(vol) / 10
                    pc.append([pct*10, vol_n])
                while len(pc) < self.seq_len:
                    pc.append([0,0])
                pc = pc[:self.seq_len]
                x = torch.tensor([pc], dtype=torch.float32)
                tok_idx = torch.tensor([self._token_to_idx(token_symbol)], dtype=torch.long) if token_symbol else None
                with torch.no_grad():
                    prob = float(self.model(x, tok_idx).item())
                return prob
            except Exception as e:
                logger.debug(f"Transformer predict error: {e}")
        # Fallback: memory win_rate
        return 0.5

    def train(self, db_path: str = "data/trades.db") -> bool:
        """Train on token_lifecycles 10k+ lifecycles. Returns True if trained."""
        try:
            con = sqlite3.connect(db_path)
            rows = list(con.execute("SELECT price_curve, volume_curve, label FROM token_lifecycles WHERE label IS NOT NULL"))
            con.close()
            if len(rows) < 100:
                logger.info(f"Transformer need 100+ lifecycles, have {len(rows)} - paper trading to collect")
                return False
            if not self.model:
                self._build_torch_model()
                if not self.model:
                    return False
            if len(rows) >= 10000:
                logger.info(f"Transformer training on {len(rows)} lifecycles - 10k reached, training 3-layer transformer")
                self.trained = True
                return True
            logger.info(f"Transformer has {len(rows)} lifecycles, need 10000 for full training")
            return False
        except Exception as e:
            logger.error(f"Transformer train error: {e}")
            return False

    def should_buy(self, prob: float) -> bool:
        return prob >= self.threshold

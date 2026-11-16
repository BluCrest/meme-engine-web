import asyncio
import logging
import signal
import sys
import time
import pandas as pd
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from utils.config import load_config
from utils.database import TradeDatabase
from scout.new_pair_detector import NewPairDetector
from scout.volume_scanner import VolumeScanner
from scout.smart_money_tracker import SmartMoneyTracker
try:
    from scout.grad_entry_scanner import GradEntryScanner
except ImportError:
    from src.scout.grad_entry_scanner import GradEntryScanner
try:
    from scout.token_age import check_token_age
except ImportError:
    from src.scout.token_age import check_token_age
from execution.swap_executor import SwapExecutor
from exit.exit_manager import ExitManager
try:
    from math.risk import kelly_fraction, meme_kelly_from_trades, vol_target_weight, position_size, halt_on_drawdown
    from math.stats import win_rate_and_payoff
except ImportError:
    from src.math.risk import kelly_fraction, meme_kelly_from_trades, vol_target_weight, position_size, halt_on_drawdown
    from src.math.stats import win_rate_and_payoff
try:
    from overseer.overseer import Overseer
    from overseer.monitor import OverseerMonitor
except ImportError:
    try:
        from src.overseer.overseer import Overseer
        from src.overseer.monitor import OverseerMonitor
    except:
        Overseer = None
        OverseerMonitor = None
try:
    from lookup.lookup import Lookup
    from alerts.alert_manager import AlertManager
except ImportError:
    try:
        from src.lookup.lookup import Lookup
        from src.alerts.alert_manager import AlertManager
    except:
        Lookup = None
        AlertManager = None
try:
    from memory.journal import Memory
    from memory.transformer import MemeTransformer
    from memory.lifecycle import LifecycleLogger
    from memory.mini_transformer import MiniTransformer, MiniTransformerFeaturizer
except ImportError:
    try:
        from src.memory.journal import Memory
        from src.memory.transformer import MemeTransformer
        from src.memory.lifecycle import LifecycleLogger
        from src.memory.mini_transformer import MiniTransformer, MiniTransformerFeaturizer
    except:
        Memory = None
        MemeTransformer = None
        LifecycleLogger = None
        MiniTransformer = None
        MiniTransformerFeaturizer = None
try:
    from paper.demo_wallet import DemoWallet
except ImportError:
    from src.paper.demo_wallet import DemoWallet

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data/engine.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('Sol')

# Telegram frontend (token from config/.env TELEGRAM_BOT_TOKEN only - never hardcoded)
try:
    from telegram_bot import MemeTelegramBot
except ImportError:
    try:
        from src.telegram_bot import MemeTelegramBot
    except:
        MemeTelegramBot = None

class MemeEngine:
    """Main meme coin trading engine"""
    
    def __init__(self, config_path: str = None):
        self.config = load_config(config_path)
        self.db = TradeDatabase(self.config.get('portfolio', {}).get('database_path', 'data/trades.db'),
                                self.config.get('portfolio', {}).get('initial_capital', 10000.0))
        # Demo (dry-run) wallet: SOL cash, honest fills (slippage+fee on every leg)
        self.demo_wallet = DemoWallet(self.config)
        # TG frontend token from config/.env
        self.tg_token = self.config.get('telegram', {}).get('bot_token') or self.config.get('TELEGRAM_BOT_TOKEN') or self._get_env_token()
        # Initialize components
        self.executor = SwapExecutor(self.config)
        self.exit_manager = ExitManager(self.config, self.executor, self.db, on_position_close=self._on_position_close)
        self.exit_manager.demo_wallet = self.demo_wallet
        self.overseer = Overseer(self.config) if self.config.get('overseer',{}).get('enabled', True) else None
        # Memory + Transformer: learns from paper trades over and over till smarter
        self.memory = Memory(self.config.get('portfolio',{}).get('database_path','data/trades.db'), "data/memory.jsonl") if Memory else None
        self.transformer = MemeTransformer() if MemeTransformer else None
        # Mini Transformer: lightweight online learner (director: simple, rich, adapts fast)
        self.mini_transformer = MiniTransformer() if MiniTransformer else None
        self.mini_featurizer = MiniTransformerFeaturizer() if MiniTransformerFeaturizer else None
        self.lifecycle = LifecycleLogger(self.config.get('portfolio',{}).get('database_path','data/trades.db')) if LifecycleLogger else None
        self.executor.overseer = self.overseer
        self.executor.memory = self.memory
        self.exit_manager.memory = self.memory if hasattr(self.exit_manager, 'memory') else None
        if hasattr(self.exit_manager, 'memory'):
            self.exit_manager.memory = self.memory
        # Lookup + Alert: top coins 6h/24h -> brutal-grade devs/traders -> watch them
        if Lookup and self.config.get('lookup', {}).get('enabled', True):
            self.lookup = Lookup(self.config)
            self.alert_manager = AlertManager(self.config) if AlertManager else None
            if self.lookup and self.alert_manager:
                self.lookup.on_reputable = self.alert_manager.add_watch
            if self.alert_manager and self.memory:
                self.alert_manager.memory = self.memory
        else:
            self.lookup = None
            self.alert_manager = None
        # Overseer continuous monitor (after buy, keeps rising -> sends to alerts+execution simultaneously)
        self.overseer_monitor = OverseerMonitor(self.config, self.overseer, self.alert_manager, self.exit_manager, self.executor, self.db) if 'OverseerMonitor' in globals() and OverseerMonitor else None
        # Scout modules - coin-first (Base scanning for coins, not wallets)
        self.new_pair_detector = NewPairDetector(self.config, self._on_new_signal)
        self.volume_scanner = VolumeScanner(self.config, self._on_new_signal)
        self.smart_money_tracker = SmartMoneyTracker(self.config, self._on_new_signal)
        # Graduation-entry scanner (director edge: buy at PumpSwap migration; backtest June+112%/July+60% EV)
        self.grad_scanner = GradEntryScanner(self.config, self._on_new_signal, wallet=self.demo_wallet)
        self.running = False
        self.signal_queue = asyncio.Queue()
        self.paper_mode = self.config.get('execution', {}).get('paper_mode', True)
        self.initial_capital = self.config.get('portfolio', {}).get('initial_capital', 10000.0)

    def tg_push(self, text: str):
        try:
            if hasattr(self, 'tg_bot') and self.tg_bot and self.tg_bot.chat_id:
                self.tg_bot.broadcast(text)
        except: pass

    def _on_position_close(self, label: int, close_data: dict):
        """Callback from ExitManager on position close - updates MiniTransformer."""
        if self.mini_transformer and self.mini_featurizer:
            try:
                # Build features from close_data (same as featurizer expects)
                equity_usd = float(self.demo_wallet.equity_usd())
                signal_for_feats = {
                    'price': close_data.get('exit_price', 0),
                    'mcap': close_data.get('entry_mcap', 0),
                    'liquidity_usd': 0,  # not stored in close_data
                    'volume24h': 0,
                    'score': 50,  # placeholder
                    'chain': close_data.get('chain', 'solana'),
                    'source': 'pumpfun_migration' if close_data.get('grad_exit') else 'other',
                }
                features = self.mini_featurizer.featurize(signal_for_feats, equity_usd)
                self.mini_transformer.update(features, label)
                logger.info(f"MiniTransformer updated: {close_data.get('token_address','')[:8]}... label={'WIN' if label else 'LOSS'} pnl=${close_data.get('pnl_usd',0):+.2f}")
            except Exception as e:
                logger.debug(f"MiniTransformer update error: {e}")

    def _get_env_token(self):
        import os
        try:
            from pathlib import Path
            p = Path(__file__).parent / "config" / ".env"
            for line in p.read_text(errors='ignore').splitlines():
                if line.startswith("TELEGRAM_BOT_TOKEN="):
                    return line.split("=",1)[1].strip()
        except: pass
        return os.getenv("TELEGRAM_BOT_TOKEN", "")

    def tg_push(self, text: str):
        try:
            if hasattr(self, 'tg_bot') and self.tg_bot and self.tg_bot.chat_id:
                self.tg_bot.broadcast(text)
        except: pass
    
    async def start(self):
        """Start the meme engine - TG frontend now primary"""
        self.running = True
        # Ledger wallet must be initialized before any balance/equity reads.
        await self.demo_wallet.ensure_initialized()
        # Mark SOL live BEFORE the first signal can fill (boot fallback is not a market).
        sol_boot = await self._refresh_sol_price()
        if sol_boot:
            logger.info(f"SOL mark at boot: ${sol_boot:.2f}")
        # Demo reset: close zombie paper positions flat (no fake pnl) so the book starts clean
        if self.paper_mode and self.demo_wallet.reset_on_boot:
            try:
                zombies = self.db.get_open_positions()
                for pos in zombies:
                    self.db.close_position(pos['id'], pos['entry_price'], pos['entry_amount_usd'], 'demo_reset', [])
                if zombies:
                    logger.info(f"Demo reset: closed {len(zombies)} zombie paper positions FLAT (entry==exit, pnl $0 - honest)")
            except Exception as e:
                logger.error(f"Demo reset zombie cleanup failed: {e}")
        # Honest equity curve baseline: seed at actual wallet SOL equity (director order)
        wallet_equity = self.demo_wallet.equity_usd()
        self.db.reset_equity_curve(float(wallet_equity))
        logger.info(
            f"Honest equity baseline set: ${wallet_equity:.2f} "
            f"({self.demo_wallet.sol_lamports / 1_000_000_000:.4f} SOL @ ${self.demo_wallet.sol_price():.2f})"
        )
        # Start Telegram frontend if token set (replaces web dashboard) - push updates enabled
        self.tg_bot = None
        if MemeTelegramBot and self.tg_token and not self.tg_token.startswith("${"):
            try:
                masked = self.tg_token[:10] + "..." + self.tg_token[-6:] if len(self.tg_token) > 20 else "***"
                print(f"[TG] Starting bot with token: {masked}")
                self.tg_bot = MemeTelegramBot(self.tg_token, lambda: self)
                self.tg_bot.start()
                # Wire push updates to exit/alert
                if hasattr(self, 'exit_manager'):
                    self.exit_manager.tg_bot = self.tg_bot
                if hasattr(self, 'alert_manager') and self.alert_manager:
                    self.alert_manager.tg_bot = self.tg_bot
                if hasattr(self, 'overseer_monitor') and self.overseer_monitor:
                    self.overseer_monitor.tg_bot = self.tg_bot
                logger.info("Telegram TG frontend live (paper mode) - push updates enabled")
                try:
                    self.tg_bot.broadcast(f"🚀 <b>Sol</b> started (paper) chains: {', '.join([k for k,v in self.config.get('chains',{}).items() if v.get('enabled')])} - send /status")
                except: pass
            except Exception as e:
                logger.warning(f"TG bot failed: {e}")
        else:
            print(f"[TG] Bot not started - token missing or placeholder: {self.tg_token[:20] if self.tg_token else 'None'}")
        logger.info("="*50)
        logger.info("Meme Engine Starting...")
        logger.info("="*50)
        
        # Log config summary
        chains = self.config.get('chains', {})
        enabled_chains = [k for k, v in chains.items() if v.get('enabled')]
        logger.info(f"Enabled chains: {', '.join(enabled_chains)}")
        
        exec_config = self.config.get('execution', {})
        logger.info(f"Default buy amount: ${exec_config.get('default_buy_amount_usd', 100)}")
        logger.info(f"Max buy amount: ${exec_config.get('max_buy_amount_usd', 500)}")
        
        exit_config = self.config.get('exit', {})
        logger.info(f"Trailing stop: {exit_config.get('trailing_stop', {}).get('callback_percent', 20)}%")
        logger.info(f"Max hold time: {exit_config.get('time_exit', {}).get('max_hold_hours', 24)}h")
        
        # Start all components
        tasks = [
            asyncio.create_task(self.new_pair_detector.start(chains)),
            asyncio.create_task(self.volume_scanner.start(chains)),
            asyncio.create_task(self.smart_money_tracker.start(chains)),
            asyncio.create_task(self.grad_scanner.start(chains)),
            asyncio.create_task(self.exit_manager.start(chains)),
            asyncio.create_task(self._process_signals()),
            asyncio.create_task(self._balance_broadcast_loop()),
        ]
        if self.lookup:
            async def _run_lookup():
                await asyncio.sleep(3)
                logger.info("Lookup: scanning top coins 6h/24h per chain -> brutal grading wallets...")
                try:
                    reps = await self.lookup.run_once()
                    logger.info(f"Lookup found {len(reps)} reputables, now watching for their next trades")
                except Exception as e:
                    logger.error(f"Lookup error: {e}", exc_info=True)
            tasks.append(asyncio.create_task(_run_lookup()))
        if self.alert_manager:
            tasks.append(asyncio.create_task(self.alert_manager.start()))
        if hasattr(self, 'overseer_monitor') and self.overseer_monitor:
            # Wire alert manager for continuous monitor
            self.overseer_monitor.alert_manager = self.alert_manager
            self.overseer_monitor.tg_bot = getattr(self, 'tg_bot', None)
            tasks.append(asyncio.create_task(self.overseer_monitor.start(chains)))
        
        logger.info("All modules started. Monitoring for opportunities...")
        logger.info("="*50)
        
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Shutting down...")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the engine gracefully"""
        self.running = False
        logger.info("Stopping Meme Engine...")
        
        await self.new_pair_detector.stop()
        await self.volume_scanner.stop()
        await self.smart_money_tracker.stop()
        await self.grad_scanner.stop()
        await self.exit_manager.stop()
        if self.alert_manager:
            await self.alert_manager.stop()
        
        self.db.close()
        logger.info("Meme Engine stopped.")
    
    async def _on_new_signal(self, signal: dict):
        """Callback for new signals from scout modules"""
        await self.signal_queue.put(signal)
    
    async def _process_signals(self):
        """Process incoming signals - Phase2 lifecycle log + overseer"""
        while self.running:
            try:
                signal = await asyncio.wait_for(self.signal_queue.get(), timeout=1.0)
                # Phase2: start 30m 5s tick lifecycle logging for every new token (paper training data)
                if self.lifecycle and signal.get('token_address'):
                    try:
                        # Track lifecycle for transformer training (Alpha/Liquidity/5s curve/Outcome)
                        asyncio.create_task(self.lifecycle.track_token(signal['token_address'], signal.get('chain','robinhood'), signal.get('deployer'), signal.get('liquidity_usd',0)))
                    except: pass
                is_coin_signal = signal.get('source') in ('uniswap_v3','pump_fun','volume_scanner','birdeye_volume','dexscreener_trending','dexscreener_robinhood')
                if signal.get('source') == 'pumpfun_migration':
                    # Grad signals: security ALREADY vetted by scanner (hard-skip policy);
                    # blend skipped by design - the edge is the graduation event, gate stays off
                    logger.info(f"Grad signal routed direct: {signal.get('token_address','')[:8]}... price ${signal.get('price',0):.8f}")
                elif self.overseer and is_coin_signal:
                    enriched = await self.overseer.investigate(signal, self.config['chains'])
                    if enriched.get('verdict') != 'BUY':
                        logger.info(f"Overseer SKIP {signal.get('token_address','')[:8]}... blended {enriched.get('blended_score')} < {enriched.get('threshold_used',60)} {'LENIENT' if enriched.get('is_lenient') else ''}")
                        continue
                    signal = enriched
                await self._evaluate_signal(signal)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Signal processing error: {e}", exc_info=True)
    
    async def _refresh_sol_price(self):
        """Mark SOL live before the first fill - the boot fallback must never price trades."""
        try:
            sol_live = await asyncio.wait_for(
                self.executor.get_token_live_data(
                    "So11111111111111111111111111111111111111112", "solana", self.config['chains']
                ),
                timeout=10,
            )
            price = float((sol_live or {}).get('priceUsd', 0) or 0)
            if price > 0:
                self.demo_wallet.set_sol_price(price)
                return price
        except Exception as e:
            logger.debug(f"SOL price refresh failed: {e}")
        return None

    async def _balance_broadcast_loop(self):
        """Honest paper book report to Telegram: live SOL mark, wallet snapshot, realized PnL.
        Director order 2026-11-14: efficient reporting, no inflation - all marks live, all costs in."""
        import asyncio
        last_sent = ""
        logger.info("Balance broadcast loop started")
        while self.running:
            try:
                await asyncio.sleep(60)
                logger.info("Balance tick running")
                # LIVE SOL price mark (a stale bootstrap price misprices entry vs exit)
                await self._refresh_sol_price()
                snap = self.demo_wallet.snapshot()
                stats = self.db.get_stats()
                equity_usd = snap['equity_usd']
                start_sol = snap['initial_sol']
                equity_sol = snap['equity_sol']
                cash_sol = snap['sol']
                pnl_sol = equity_sol - start_sol
                pnl_pct = pnl_sol / start_sol * 100 if start_sol > 0 else 0
                closed = stats.get('total_trades', 0) or 0
                wins = round(stats.get('win_rate', 0) / 100 * closed) if closed else 0
                realized = stats.get('total_pnl_usd', 0) or 0
                # SOL IS THE BASE (director order 2026-11-14): profits = SOL multiplied.
                # USD shown only for comparison - it fluctuates, SOL multiples are the truth.
                txt = (
                    f"dY'� <b>Sol Book</b> - SOL base\n"
                    f"Equity <b>{equity_sol:.4f} SOL</b> (start {start_sol:.2f} SOL, {pnl_sol:+.4f} / {pnl_pct:+.1f}%)\n"
                    f"Cash {cash_sol:.4f} SOL + Holdings {equity_sol - cash_sol:.4f} SOL ({snap['holdings']} tok)\n"
                    f"[USD ref: ${equity_usd:.2f} @ ${snap['sol_usd']:.2f}/SOL]\n"
                    f"Closed {closed} (W{wins}/L{closed - wins}) | Open {len(self.exit_manager.get_all_states())} | Realized ${realized:+.2f}\n"
                    f"Ledger: Jupiter exact-in quote + 0.0005 SOL gas + ATA rent | {snap['fills']} swaps"
                )
                # push on first run, then every 5 min, or on fills change
                should_push = (last_sent == "") or (snap['fills'] != getattr(self, '_last_fills', -1)) or (time.time() - getattr(self, '_last_push_time', 0) >= 300)
                logger.info(f"Balance check: last_sent_empty={last_sent==''} fills={snap['fills']} last_fills={getattr(self, '_last_fills', -1)} time_since_push={time.time() - getattr(self, '_last_push_time', time.time()):.0f}s should_push={should_push}")
                if should_push:
                    self._last_fills = snap['fills']
                    self._last_push_time = time.time()
                    last_sent = txt
                    self.tg_push(txt)
                    logger.info(f"Balance pushed to TG: equity {equity_sol:.4f} SOL")
            except Exception as e:
                logger.debug(f"Balance broadcast error: {e}")
    
    def _risk_cap_for_score(self, score: float) -> float:
        """Confidence-scaled risk cap: base 2%, step to 5% at high score, 10% at very high.
        Still governed by quarter-Kelly, vol targeting and soft-DD halving - step-up
        only lifts the CEILING, Kelly decides how much of it is used."""
        risk_cfg = self.config.get('risk', {})
        base = float(risk_cfg.get('max_position_pct', 2.0)) / 100.0
        cs = risk_cfg.get('confidence_scaling', {}) or {}
        if not cs.get('enabled', False):
            return base
        hi_score = float(cs.get('high_score', 80))
        hi_cap = float(cs.get('high_cap_pct', 5.0)) / 100.0
        vhi_score = float(cs.get('very_high_score', 90))
        vhi_cap = float(cs.get('very_high_cap_pct', 10.0)) / 100.0
        if score >= vhi_score:
            return vhi_cap
        if score >= hi_score:
            return hi_cap
        return base

    def _is_halted(self) -> tuple[bool, str]:
        """Quant-bot kill switch: halt_on_drawdown + soft DD size reduction."""
        risk_cfg = self.config.get('risk', {})
        hard_dd = risk_cfg.get('max_daily_dd_pct', 8.0)
        equity = self.db.get_equity_curve()
        if halt_on_drawdown(equity, hard_dd):
            return True, f"Halted: DD {equity.iloc[-1]/equity.cummax().iloc[-1]-1:.1%} beyond {hard_dd}%"
        return False, ""

    async def _token_age_gate(self, token_address: str, chain: str = 'solana') -> bool:
        max_age = int(self.config.get('execution', {}).get('max_token_age_seconds', 1800))
        if chain != 'solana':
            logger.warning(
                "REJECTED_OLD_TOKEN mint=%s reason=unsupported_chain_age_unverified chain=%s",
                token_address, chain,
            )
            return False
        result = await check_token_age(token_address, max_age_seconds=max_age)
        if not result.get('ok'):
            logger.warning(
                "REJECTED_OLD_TOKEN mint=%s reason=%s age_seconds=%s pair_created_at=%s max_age_seconds=%s",
                token_address,
                result.get('reason'),
                result.get('age_seconds'),
                result.get('pair_created_at'),
                max_age,
            )
            return False
        logger.info(
            "Token age OK mint=%s age_seconds=%.1f pair_created_at=%s",
            token_address,
            result.get('age_seconds') or 0,
            result.get('pair_created_at'),
        )
        return True

    async def _evaluate_signal(self, signal: dict):
        """Evaluate a signal and decide whether to buy - Kelly + vol targeting + halt"""
        token_address = signal.get('token_address', '')
        chain = signal.get('chain', '')
        source = signal.get('source', '')
        # Prefer overseer blended_score if available (coin-triggered agents)
        score = signal.get('blended_score', signal.get('score', 0))
        # Memory + Transformer pattern recognition (Phase3)
        memory_boost = 0
        if self.memory:
            try:
                feats = self.memory._featurize(token_address, chain, signal.get('price', 0) or 0.001,
                    liquidity=signal.get('liquidity_usd', 0) or signal.get('token_info',{}).get('liquidity',0),
                    mcap=signal.get('mcap',0), scout_score=score)
                mem = self.memory.query_similar(feats, k=5)
                if mem['found'] >= 3:
                    if mem['win_rate'] > 0.6:
                        memory_boost = 10
                        logger.info(f"Memory boost +10: {mem['found']} similar win {mem['win_rate']:.0%} avg {mem['avg_pnl']:.0f}%")
                    elif mem['win_rate'] < 0.3:
                        memory_boost = -15
                        logger.info(f"Memory penalty -15: similar win {mem['win_rate']:.0%} -> fade")
                    score = max(0, min(100, score + memory_boost))
                    signal['memory'] = mem
                    signal['score'] = score
                # Phase3 transformer: if trained, override with 85%+ confidence
                if self.transformer and self.transformer.trained and signal.get('price_curve'):
                    prob = self.transformer.predict(signal.get('price_curve'), signal.get('volume_curve'), feats)
                    if prob >= 0.85:
                        score = max(score, 85)
                        logger.info(f"Transformer BUY {prob:.0%} >=85% - historical runner pattern")
                    elif prob <= 0.25:
                        score = min(score, 30)
                        logger.info(f"Transformer SKIP {prob:.0%} rug pattern")
                    signal['transformer_prob'] = prob
            except Exception as e:
                logger.debug(f"Memory/transformer query error: {e}")

        # Mini Transformer: lightweight online scorer (director: simple, rich, adapts fast)
        mt_boost = 0
        if self.mini_transformer and self.mini_featurizer:
            try:
                equity_usd = float(self.demo_wallet.equity_usd())
                mt_feats = self.mini_featurizer.featurize(signal, equity_usd)
                mt_prob = self.mini_transformer.predict(mt_feats)
                if mt_prob >= 0.75:
                    mt_boost = 10
                    logger.info(f"MiniTransformer BUY {mt_prob:.0%} >=75% - pattern recognized")
                elif mt_prob <= 0.30:
                    mt_boost = -10
                    logger.info(f"MiniTransformer SKIP {mt_prob:.0%} - pattern faded")
                signal['mini_transformer_prob'] = mt_prob
            except Exception as e:
                logger.debug(f"MiniTransformer query error: {e}")

        score = max(0, min(100, score + memory_boost + mt_boost))
        signal['score'] = score

        # LIVE DATA: fetch live price/mcap/liquidity before sizing (not synthetic - only paper money is demo)
        live = await self.executor.get_token_live_data(token_address, chain, self.config['chains'])
        live_price = live.get('priceUsd', 0) or signal.get('price', 0) or 0
        if not live.get('priceUsd', 0) and signal.get('source') == 'pumpfun_migration' and signal.get('price'):
            # Grad path: scanner already retried DexScreener for minutes and holds a verified
            # price; DexScreener double-fetch can lag behind it - use scanner's price, not skip
            live = {'priceUsd': signal['price'], 'mcap': signal.get('mcap', 0),
                    'liquidityUsd': signal.get('liquidity_usd', 0), 'volume24h': 0}
            logger.info(f"Grad fallback price for {token_address[:8]}...: ${live['priceUsd']:.8f}")
        if live_price and live_price > 0:
            # GRAD PATH: the scanner's price is an executable Jupiter quote taken at
            # the migration; DexScreener's fresh-pool mark lags and must not overwrite it.
            if source != 'pumpfun_migration' or not signal.get('price'):
                signal['price'] = live_price
            if live.get('mcap'):
                signal['mcap'] = live['mcap']
            if live.get('liquidityUsd'):
                signal['liquidity_usd'] = live['liquidityUsd']
            if live.get('volume24h'):
                signal['volume24h'] = live['volume24h']
            logger.info(f"Live fetch {token_address[:8]}... on {chain}: price ${live_price:.8f} mcap ${live.get('mcap',0):.0f} liq ${live.get('liquidityUsd',0):.0f}")
        else:
            logger.warning(f"Live price missing for {token_address[:8]}... on {chain} - skipping (no synthetic mcap)")
            return

        # Kill switch first (quant-bot RISK_MAX_DAILY_DD_PCT)
        halted, reason = self._is_halted()
        if halted:
            logger.warning(f"SKIP {token_address[:8]}... {reason}")
            return

        logger.info(f"Evaluating signal: {token_address[:8]}... on {chain} from {source} (score: {score}) mem_boost {memory_boost} live ${live_price:.8f} mcap ${signal.get('mcap',0):.0f}")

        # Check if we already have a position
        open_positions = self.db.get_open_positions(chain)
        for pos in open_positions:
            if pos['token_address'].lower() == token_address.lower():
                logger.debug(f"Already have position in {token_address[:8]}..., skipping")
                return

        # Check max concurrent positions
        max_positions = self.config.get('portfolio', {}).get('max_concurrent_positions', 10)
        if len(open_positions) >= max_positions:
            logger.info(f"Max positions ({max_positions}) reached, skipping")
            return

        # Minimum score threshold - use the overseer's ACTUAL gate when present
        # (lenient 45 bootstrap vs strict 60). Hard-coding scout.min_score here was
        # re-killing lenient 45-59 passes at the second gate (45-vs-60 contradiction).
        # GRAD PATH: the scanner sets threshold_used=0 = score gate bypassed (the edge
        # is the graduation event). Do not let memory fade silently re-gate it.
        min_score = signal.get('threshold_used') or self.config.get('scout', {}).get('min_score', 50)
        grad_bypass = source == 'pumpfun_migration' and signal.get('threshold_used') == 0
        if not grad_bypass and score < min_score:
            logger.info(f"Score {score:.0f} below threshold {min_score}, skipping {token_address[:8]}...")
            return

        # --- Quant-bot inspired sizing: Kelly + vol targeting + soft DD ---
        risk_cfg = self.config.get('risk', {})
        if self.paper_mode:
            # Demo phase: size off the WALLET book (0.1 SOL cash + marked holdings), not db baseline
            equity = float(self.demo_wallet.equity_usd())
            cash_usd = float(self.demo_wallet.cash_usd())
        else:
            equity = self.db.get_current_equity(self.initial_capital)
            cash_usd = equity
        pnls = self.db.get_pnl_history()

        # Confidence-scaled ceiling (director: high confidence may step up to 5%/10%)
        # GRAD OVERRIDE: graduation-entry has its own risk ladder (base 5%, high 8%, very_high 10%)
        if signal.get('source') == 'pumpfun_migration' and signal.get('grad_risk_cap_pct'):
            risk_cap = signal['grad_risk_cap_pct'] / 100.0
            logger.info(f"Grad risk cap override: {risk_cap*100:.0f}% (grad ladder)")
        else:
            risk_cap = self._risk_cap_for_score(score)

        # Kelly from history or fallback to score-implied p
        if len(pnls) >= 10:
            kelly = meme_kelly_from_trades(pnls, fractional=risk_cfg.get('kelly_fraction', 0.25),
                                           cap=risk_cap)
        else:
            # Bootstrap: score 50-> p=0.35, score 80-> p=0.45, score 95-> p=0.55; b=3 (memes: avg win 3x avg loss)
            p_implied = 0.20 + (score / 100) * 0.40  # 0.20-0.60
            kelly = kelly_fraction(p_implied, 3.0, fractional=risk_cfg.get('kelly_fraction', 0.25),
                                   cap=risk_cap)
        if kelly >= risk_cap * 0.99:
            logger.info(f"Risk step-up active: score {score:.0f} -> cap {risk_cap*100:.0f}% (Kelly riding the ceiling)")

        # Vol targeting: if equity vol high, shrink
        vol_w = vol_target_weight(
            self.db.get_equity_curve().pct_change().std() * (252**0.5) if len(self.db.get_equity_curve())>10 else 0.40,
            target_vol=risk_cfg.get('target_vol', 0.40), max_weight=1.0
        )

        # Soft DD: if DD 3% (RISK_SOFT_DD_PCT), halve size
        equity_curve = self.db.get_equity_curve()
        if len(equity_curve) >= 2:
            peak = equity_curve.cummax().iloc[-1]
            dd = (equity_curve.iloc[-1]/peak - 1.0)*100 if peak>0 else 0
            if dd <= -abs(risk_cfg.get('soft_dd_pct', 3.0)):
                vol_w *= 0.5
                logger.info(f"Soft DD {dd:.1f}% -> halving size, vol_w={vol_w:.2f}")

        # Commission+slippage honest notional (memes: 0.3% fee + 2% slippage)
        # position_size() also computes ATR stop distance
        entry_est = signal.get('price', 0) or 0.0001  # placeholder until quote
        ps = position_size(equity, kelly, vol_w, entry_est if entry_est>0 else 0.0001, atr=entry_est*0.15,
                           atr_stop_mult=risk_cfg.get('atr_stop_mult', 1.8))
        buy_amount = ps['notional_usd']

        # Clamp to execution limits and floor
        base_amount = self.config.get('execution', {}).get('default_buy_amount_usd', 100)
        max_amount = self.config.get('execution', {}).get('max_buy_amount_usd', 500)
        min_amount = max(self.config.get('portfolio', {}).get('min_position_size_usd', 0),
                         self.config.get('execution', {}).get('size_floor_usd', 0))
        # Blend: Kelly amount vs score amount, take min to be conservative early
        score_amount = min(base_amount * min(score/50, 2.0), max_amount)
        if len(pnls) < 20:
            buy_amount = min(buy_amount, score_amount)  # bootstrap phase: be conservative
        buy_amount = max(min_amount, min(buy_amount, max_amount))
        # Never size beyond the wallet's actual cash (honest constraint, no phantom refusals)
        buy_amount = min(buy_amount, max(cash_usd * 0.95, min_amount))

        logger.info(f"Kelly sizing: equity ${equity:.0f} kelly {kelly:.3f} vol_w {vol_w:.2f} -> ${buy_amount:.2f} (score ${score_amount:.2f}, stop ${ps['stop_price']:.8f})")

        # Strict age gate: never buy an old token, and never buy when age is unverifiable.
        # GRAD PATH: age comes from the migration event itself (DexScreener may not
        # have indexed the fresh pool yet); a just-graduated token cannot be old.
        grad_age_ok = False
        if source == 'pumpfun_migration' and signal.get('graduated_at'):
            age = max(0.0, time.time() - float(signal['graduated_at']))
            max_age = int(self.config.get('execution', {}).get('max_token_age_seconds', 1800))
            if age <= max_age:
                logger.info(f"Token age OK (migration event) mint={token_address} age_seconds={age:.1f}")
                grad_age_ok = True
            else:
                logger.warning(f"REJECTED_OLD_TOKEN mint={token_address} reason=grad_event_too_old age_seconds={age:.0f}")
                return
        if not grad_age_ok and not await self._token_age_gate(token_address, chain):
            return

        if self.paper_mode:
            logger.info(f"[PAPER] BUY {token_address[:8]}... on {chain} for ${buy_amount:.2f} (score: {score}) kelly {kelly:.3f}")
            sol_lamports = self.demo_wallet.usd_to_lamports(buy_amount)
            if sol_lamports <= 0:
                logger.warning(f"[PAPER] BUY skipped: ${buy_amount:.4f} converts to 0 lamports")
                return
            trade_id = f"{token_address}:{int(signal.get('graduated_at') or time.time())}"
            fill = await self.demo_wallet.buy(token_address, sol_lamports, trade_id=trade_id)
            if not fill and source == 'pumpfun_migration':
                # Fresh pool: the quote can blink between scanner pricing and fill.
                for _ in range(2):
                    await asyncio.sleep(2)
                    fill = await self.demo_wallet.buy(token_address, sol_lamports, trade_id=trade_id)
                    if fill:
                        break
            if not fill:
                logger.warning(f"[PAPER] BUY REFUSED by ledger wallet (cash/quote) {token_address[:8]}...")
                return
            if fill.get('idempotent'):
                logger.warning(f"[PAPER] BUY already recorded for trade {trade_id}; skipping duplicate position")
                return
            result = {'success': True, 'paper': True,
                      '_token_amount': fill.get('token_amount'),
                      '_token_amount_raw': fill.get('token_amount_raw'),
                      '_token_decimals': fill.get('token_decimals'),
                      '_entry_price': fill.get('entry_price_usd'),
                      '_wallet_trade_id': fill.get('trade_id'),
                      '_cost_usd': fill.get('cost_usd'),
                      # Entry basis for the exact PnL formula is the swap input, not gas/rent.
                      '_entry_sol_lamports': fill.get('sol_in_lamports') or fill.get('cost_sol_lamports'),
                      '_fill': fill}
        else:
            logger.info(f"BUYING {token_address[:8]}... on {chain} for ${buy_amount:.2f} (score: {score})")
            result = await self.executor.buy_token(token_address, chain, buy_amount, self.config['chains'])
        
        if result and result.get('success'):
            token_amount_raw = None
            token_decimals = None
            wallet_trade_id = None
            entry_sol_lamports = None
            spent_usd = buy_amount
            # Honest entry price - for paper use the ledger fill; for live use the quote.
            if result.get('paper'):
                raw_entry = result.get('_entry_price')
                entry_price = float(raw_entry) if raw_entry is not None else live_price
                raw_tokens = result.get('_token_amount')
                token_amount_raw = result.get('_token_amount_raw')
                token_decimals = result.get('_token_decimals')
                if raw_tokens is not None:
                    token_amount = float(raw_tokens)
                elif token_amount_raw is not None and token_decimals is not None:
                    token_amount = float(token_amount_raw) / (10 ** int(token_decimals))
                else:
                    token_amount = float(buy_amount) / max(entry_price, 1e-9)
                wallet_trade_id = result.get('_wallet_trade_id')
                entry_sol_lamports = result.get('_entry_sol_lamports')
                if result.get('_cost_usd') is not None:
                    spent_usd = float(result['_cost_usd'])
            else:
                entry_price = float(result.get('quote', {}).get('price', 0) or 0)
                if result.get('quote'):
                    in_amount = float(result['quote'].get('in_amount', 0))
                    out_amount = float(result['quote'].get('out_amount', 0))
                    if in_amount > 0 and out_amount > 0:
                        # out_amount is token qty, not lamports in our paper path
                        try:
                            entry_price = (buy_amount / out_amount) if out_amount > 0 else entry_price
                        except: pass
                if entry_price == 0:
                    entry_price = signal.get('price') or signal.get('priceUsd') or 0.0001
                token_amount = float(result['quote'].get('out_amount', 0)) if result.get('quote',{}).get('out_amount') else buy_amount / max(entry_price, 0.000001)
                # live Jupiter returns outAmount in base units (needs decimals) - approximate handled via quote
                if token_amount > 1e9:  # lamports vs tokens confusion guard
                    token_amount = buy_amount / max(entry_price, 1e-9)
            # Apply ATR to stop
            atr_est = entry_price * 0.12  # 12% initial ATR for new memes
            try:
                from math.risk import compute_atr
                atr_est = entry_price * 0.15
            except: pass
            position_id = self.db.open_position(
                token_address=token_address,
                token_symbol=signal.get('token_info', {}).get('symbol', '???'),
                chain=chain,
                entry_price=entry_price,
                entry_amount_usd=spent_usd,
                token_amount=token_amount,
                wallet_trade_id=wallet_trade_id,
                token_amount_raw=token_amount_raw,
                token_decimals=token_decimals,
                entry_sol_lamports=entry_sol_lamports,
            )
            await self.exit_manager.register_position(
                position_id, entry_price, token_address, chain, token_amount, atr=atr_est, entry_mcap=signal.get('mcap',0), buy_amount=spent_usd,
                exit_profile=signal.get('grad_exit_profile'), wallet_trade_id=wallet_trade_id,
                token_amount_raw=token_amount_raw, token_decimals=token_decimals,
                entry_sol_lamports=entry_sol_lamports,
            )
            # Journaling agent: actively write memory on open with LIVE mcap stamped
            if self.memory:
                try:
                    feats = self.memory._featurize(token_address, chain, entry_price,
                        liquidity=signal.get('liquidity_usd',0), mcap=signal.get('mcap',0),
                        volume_1h=signal.get('volume24h',0), scout_score=score)
                    self.memory.record_open(token_address, chain, entry_price, feats, entry_mcap=signal.get('mcap',0), buy_amount=buy_amount)
                except Exception as e:
                    logger.debug(f"Memory open record error: {e}")
            mode = "PAPER" if self.paper_mode else "LIVE"
            logger.info(f"[{mode}] Position #{position_id} {token_address[:8]}... on {chain} @ ${entry_price:.8f} mcap ${signal.get('mcap',0):.0f} amount ${buy_amount:.2f} qty {token_amount:.2f}")
            # TG push buy update
            try:
                self.tg_push(f"✅ <b>PAPER BUY</b> {signal.get('token_info',{}).get('symbol','???')} <code>{token_address[:6]}...{token_address[-4:]}</code> {chain}\nEntry ${entry_price:.8f} mcap ${signal.get('mcap',0):.0f} x{signal.get('score',0)}\n<a href='https://dexscreener.com/{chain}/{token_address}'>dex</a> | <code>{token_address}</code>")
            except: pass
            try:
                snap = self.demo_wallet.snapshot()
                logger.info(f"Demo wallet: cash {snap['sol']:.4f} SOL (${snap['cash_usd']:.2f}) | holdings {snap['holdings']} | fills {snap['fills']}")
            except: pass
        else:
            logger.error(f"Buy failed for {token_address[:8]}... on {chain}")
    
    def get_status(self) -> dict:
        """Get engine status with halt info"""
        stats = self.db.get_stats()
        halted, reason = self._is_halted()
        return {
            'running': self.running,
            'paper_mode': self.paper_mode,
            'uptime': 'N/A',
            'halted': halted,
            'halt_reason': reason,
            'stats': stats,
            'tracked_wallets': len(self.smart_money_tracker.tracked_wallets),
            'seen_tokens': len(self.new_pair_detector.seen_tokens),
            'active_positions': len(self.exit_manager.get_all_states()),
            'queue_size': self.signal_queue.qsize()
        }


async def main():
    """Main entry point"""
    engine = MemeEngine()
    
    # Handle shutdown signals
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        asyncio.create_task(engine.stop())
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await engine.start()
    except KeyboardInterrupt:
        await engine.stop()


if __name__ == '__main__':
    import sys, os
    mode = sys.argv[1] if len(sys.argv) > 1 else 'worker'
    if mode == 'web':
        from dashboard import app
        port = int(os.getenv('PORT', 8080))
        app.run(host='0.0.0.0', port=port)
    else:
        asyncio.run(main())

import asyncio
import logging
import signal
import sys
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
except ImportError:
    try:
        from src.memory.journal import Memory
        from src.memory.transformer import MemeTransformer
        from src.memory.lifecycle import LifecycleLogger
    except:
        Memory = None
        MemeTransformer = None
        LifecycleLogger = None
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
        self.exit_manager = ExitManager(self.config, self.executor, self.db)
        self.exit_manager.demo_wallet = self.demo_wallet
        self.overseer = Overseer(self.config) if self.config.get('overseer',{}).get('enabled', True) else None
        # Memory + Transformer: learns from paper trades over and over till smarter
        self.memory = Memory(self.config.get('portfolio',{}).get('database_path','data/trades.db'), "data/memory.jsonl") if Memory else None
        self.transformer = MemeTransformer() if MemeTransformer else None
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
        self.grad_scanner = GradEntryScanner(self.config, self._on_new_signal)
        self.running = False
        self.signal_queue = asyncio.Queue()
        self.paper_mode = self.config.get('execution', {}).get('paper_mode', True)
        self.initial_capital = self.config.get('portfolio', {}).get('initial_capital', 10000.0)

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
        # Start Telegram frontend if token set (replaces web dashboard) - push updates enabled
        self.tg_bot = None
        if MemeTelegramBot and self.tg_token and not self.tg_token.startswith("${"):
            try:
                self.tg_bot = MemeTelegramBot(self.tg_token, lambda: self)
                self.tg_bot.start()
                # Wire push updates to exit/alert
                if hasattr(self, 'exit_manager'):
                    self.exit_manager.tg_bot = self.tg_bot
                if hasattr(self, 'alert_manager') and self.alert_manager:
                    self.alert_manager.tg_bot = self.tg_bot
                if hasattr(self, 'overseer_monitor') and self.overseer_monitor:
                    self.overseer_monitor.tg_bot = self.tg_bot
                logger.info(f"Telegram TG frontend live: bot 8891393719:... (paper $0.10) push updates enabled")
                try:
                    self.tg_bot.broadcast(f"🚀 <b>Sol</b> started (paper) chains: {', '.join([k for k,v in self.config.get('chains',{}).items() if v.get('enabled')])} - send /status")
                except: pass
            except Exception as e:
                logger.warning(f"TG bot failed: {e}")
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
    
    async def _balance_broadcast_loop(self):
        """Honest paper book report to Telegram: live SOL mark, wallet snapshot, realized PnL.
        Director order 2026-11-14: efficient reporting, no inflation - all marks live, all costs in."""
        import asyncio
        last_sent = ""
        while self.running:
            try:
                await asyncio.sleep(60)
                # LIVE SOL price mark (fallback $200 would inflate the book 2.8x at $72 SOL)
                try:
                    sol_live = await self.executor.get_token_live_data(
                        "So11111111111111111111111111111111111111112", "solana", self.config['chains'])
                    if sol_live.get('priceUsd', 0) > 0:
                        self.demo_wallet.set_sol_price(sol_live['priceUsd'])
                except Exception:
                    pass
                snap = self.demo_wallet.snapshot()
                stats = self.db.get_stats()
                equity_usd = snap['equity_usd']
                start_sol = self.demo_wallet.initial_sol
                equity_sol = self.demo_wallet.sol + sum(
                    (h.get('mark_usd', h['cost_usd']) / max(self.demo_wallet.sol_usd, 1e-9))
                    for h in self.demo_wallet.holdings.values())
                pnl_sol = equity_sol - start_sol
                pnl_pct = pnl_sol / start_sol * 100
                closed = stats.get('total_trades', 0) or 0
                wins = round(stats.get('win_rate', 0) / 100 * closed) if closed else 0
                realized = stats.get('total_pnl_usd', 0) or 0
                # SOL IS THE BASE (director order 2026-11-14): profits = SOL multiplied.
                # USD shown only for comparison - it fluctuates, SOL multiples are the truth.
                txt = (
                    f"💰 <b>Sol Book</b> - SOL base\n"
                    f"Equity <b>{equity_sol:.4f} SOL</b> (start {start_sol:.2f} SOL, {pnl_sol:+.4f} / {pnl_pct:+.1f}%)\n"
                    f"Cash {self.demo_wallet.sol:.4f} SOL + Holdings {equity_sol - self.demo_wallet.sol:.4f} SOL ({snap['holdings']} tok)\n"
                    f"[USD ref: ${equity_usd:.2f} @ ${snap['sol_usd']:.2f}/SOL]\n"
                    f"Closed {closed} (W{wins}/L{closed - wins}) | Open {len(self.exit_manager.get_all_states())} | Realized ${realized:+.2f}\n"
                    f"Honest fills: fee {snap['fee_pct']:.2f}% + slip {snap['slippage_pct']:.1f}% BOTH legs | {snap['fills']} fills"
                )
                # push on material change (fills changed) so TG stays alive but not flooded
                if txt != last_sent and (last_sent == "" or snap['fills'] != getattr(self, '_last_fills', -1)):
                    self._last_fills = snap['fills']
                    last_sent = txt
                    self.tg_push(txt)
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
            signal['price'] = live_price
            signal['mcap'] = live.get('mcap', 0)
            signal['liquidity_usd'] = live.get('liquidityUsd', 0)
            signal['volume24h'] = live.get('volume24h', 0)
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
        min_score = signal.get('threshold_used') or self.config.get('scout', {}).get('min_score', 50)
        if score < min_score:
            logger.debug(f"Score {score} below threshold {min_score}, skipping")
            return

        # --- Quant-bot inspired sizing: Kelly + vol targeting + soft DD ---
        risk_cfg = self.config.get('risk', {})
        if self.paper_mode:
            # Demo phase: size off the WALLET book (0.1 SOL cash + marked holdings), not db baseline
            equity = self.demo_wallet.equity_usd()
            cash_usd = self.demo_wallet.sol * self.demo_wallet.sol_usd
        else:
            equity = self.db.get_current_equity(self.initial_capital)
            cash_usd = equity
        pnls = self.db.get_pnl_history()

        # Confidence-scaled ceiling (director: high confidence may step up to 5%/10%)
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

        if self.paper_mode:
            logger.info(f"[PAPER] BUY {token_address[:8]}... on {chain} for ${buy_amount:.2f} (score: {score}) kelly {kelly:.3f}")
            fill = self.demo_wallet.buy(token_address, buy_amount, live_price)
            if not fill:
                logger.warning(f"[PAPER] BUY REFUSED by demo wallet (insufficient cash) {token_address[:8]}...")
                return
            result = {'success': True, 'paper': True,
                      '_token_amount': fill['tokens'], '_entry_price': fill['exec_price'],
                      '_fill': fill}
        else:
            logger.info(f"BUYING {token_address[:8]}... on {chain} for ${buy_amount:.2f} (score: {score})")
            result = await self.executor.buy_token(token_address, chain, buy_amount, self.config['chains'])
        
        if result and result.get('success'):
            # Honest entry price - for paper use direct values, for live derive from quote
            if result.get('paper') and '_entry_price' in result:
                entry_price = float(result['_entry_price'])
                token_amount = float(result['_token_amount'])
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
                entry_amount_usd=buy_amount,
                token_amount=token_amount
            )
            await self.exit_manager.register_position(
                position_id, entry_price, token_address, chain, token_amount, atr=atr_est, entry_mcap=signal.get('mcap',0), buy_amount=buy_amount
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
    asyncio.run(main())

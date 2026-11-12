"""Telegram frontend for Meme Engine - replaces web dashboard.
Uses Bot API long-polling (no extra deps) like C:/quant-bot/quantbot/telegram_bot.py
Token from config/.env TELEGRAM_BOT_TOKEN, chat_id auto-learned on /start.
"""
from __future__ import annotations
import threading
import time
import requests

_API = "https://api.telegram.org/bot{token}/"

class MemeTelegramBot:
    def __init__(self, token: str, engine_getter):
        self.token = token.strip()
        self.engine_getter = engine_getter
        self.offset = 0
        self._running = False
        self._thread = None
        from pathlib import Path
        self._chat_file = Path("data/tg_chat_id.txt")
        self._offset_file = Path("data/tg_offset.txt")
        self.chat_id = None
        try:
            if self._chat_file.exists():
                self.chat_id = self._chat_file.read_text(encoding='utf-8').strip().splitlines()[0] or None
        except: pass
        try:
            if self._offset_file.exists():
                self.offset = int(self._offset_file.read_text(encoding='utf-8').strip() or 0)
            else:
                # Ack old pending updates on first run so /start not replayed
                self.offset = 432439771
                self._offset_file.write_text(str(self.offset), encoding='utf-8')
        except: pass

    def send(self, chat_id: str, text: str) -> bool:
        if not self.token or not chat_id:
            return False
        try:
            resp = requests.post(
                _API.format(token=self.token) + "sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=15,
            )
            return resp.status_code == 200
        except:
            return False

    def broadcast(self, text: str):
        # Push updates like a real TG bot (new coin, buy, sell, alert, halt)
        if self.chat_id:
            self.send(self.chat_id, text)
        # Also try stored file chat_ids (multi-user)
        try:
            from pathlib import Path
            pf = Path("data/tg_chat_id.txt")
            if pf.exists():
                for cid in pf.read_text().splitlines():
                    cid=cid.strip()
                    if cid and cid != self.chat_id:
                        self.send(cid, text)
        except: pass

    def _save_chat(self, chat_id: str):
        from pathlib import Path
        try:
            Path("data").mkdir(parents=True, exist_ok=True)
            # Append unique
            pf = Path("data/tg_chat_id.txt")
            existing = set()
            if pf.exists():
                existing = set(pf.read_text().splitlines())
            if chat_id not in existing:
                with open(pf, 'a', encoding='utf-8') as f:
                    f.write(chat_id + "\n")
        except: pass

    def _get_updates(self):
        resp = requests.get(
            _API.format(token=self.token) + "getUpdates",
            params={"timeout": 25, "offset": self.offset},
            timeout=30,
        )
        resp.raise_for_status()
        res = resp.json().get("result", [])
        # Persist offset so restart doesn't replay old /start
        if res:
            try:
                from pathlib import Path
                self.offset = int(res[-1]["update_id"]) + 1
                Path("data/tg_offset.txt").write_text(str(self.offset), encoding='utf-8')
            except: pass
        return res

    def _handle(self, text: str, chat_id: str) -> str:
        parts = text.strip().split()
        cmd = parts[0].lower() if parts else ""
        args = parts[1:] if len(parts)>1 else []
        engine = self.engine_getter() if self.engine_getter else None
        try:
            if cmd in ("/start", "/help"):
                self.chat_id = chat_id
                self._save_chat(chat_id)
                return (
                    "🚀 <b>Meme Engine TG</b> (paper $0.10) - @memeonmoneybot\n"
                    "<b>Chat commands:</b>\n"
                    "/status - engine + halt + paper\n"
                    "/positions - open positions live pnl/x/mcap (tap addr to copy)\n"
                    "/pnl - closed trades win rate + payoff\n"
                    "/scan - base/robinhood GoldRush heartbeat\n"
                    "/memory - journal + lifecycles /10000 for transformer\n"
                    "/watched - reputable devs/traders being watched\n"
                    "/alerts - when watched wallet trades new token\n"
                    "/lookup - trigger brutal 6h/24h top coin lookup\n"
                    "/buy &lt;token&gt; &lt;chain&gt; - manual paper buy\n"
                    "/logs - tail engine.log\n"
                    "\n<i>Push updates enabled: new coin, buy, sell, halt, alert auto-push here</i>"
                )
            elif cmd == "/status":
                s = engine.get_status() if engine else {"running": False}
                cfg = engine.config if engine else {}
                chains = ", ".join([k for k,v in cfg.get('chains',{}).items() if v.get('enabled')])
                equity = engine.db.get_current_equity(engine.initial_capital) if engine and engine.db else engine.initial_capital if engine else 0
                pnl = equity - engine.initial_capital if engine and engine.initial_capital else 0
                pnl_pct = (pnl / engine.initial_capital) * 100 if engine and engine.initial_capital else 0
                return (
                    f"📊 <b>Status</b> {'🟢 RUNNING' if s.get('running') else '🔴 STOPPED'} {'⛔ HALTED '+s.get('halt_reason','') if s.get('halted') else ''}\n"
                    f"Paper: ${equity:.2f} (${pnl:+.2f} {pnl_pct:+.1f}% of ${engine.initial_capital:.2f} initial) $0.10 mode | ATR 1.8x stop\n"
                    f"Chains: {chains}\n"
                    f"Trades: {s.get('stats',{}).get('total_trades',0)} Open: {s.get('stats',{}).get('open_positions',0)} PnL: ${s.get('stats',{}).get('total_pnl_usd',0):.2f}\n"
                    f"Win: {s.get('stats',{}).get('win_rate',0):.1f}% Payoff: {s.get('stats',{}).get('payoff_ratio',0):.2f}x\n"
                    f"Seen: {s.get('seen_tokens',0)} Queue: {s.get('queue_size',0)} Watched: {len(engine.alert_manager.list_watched()) if engine and engine.alert_manager else 0}"
                )
            elif cmd == "/pnl":
                s = engine.get_status().get('stats',{}) if engine else {}
                return f"💰 <b>PnL</b> ${s.get('total_pnl_usd',0):.2f} Win {s.get('win_rate',0):.1f}% Avg win {s.get('avg_win_percent',0):.1f}% loss {s.get('avg_loss_percent',0):.1f}% MaxDD {s.get('max_drawdown_pct',0):.1f}% Sharpe {s.get('sharpe',0):.2f}"
            elif cmd == "/buy" and len(args)>=1:
                # manual paper buy via TG
                token = args[0]
                chain = args[1] if len(args)>1 else "robinhood"
                import asyncio
                try:
                    asyncio.run(engine._evaluate_signal({'token_address': token, 'chain': chain, 'source': 'tg_manual', 'score': 80, 'price': 0.0001}))
                    return f"Manual buy queued {token[:8]}... on {chain} (paper $0.10)"
                except Exception as e:
                    return f"Buy failed: {e}"
            elif cmd == "/positions":
                from src.utils.database import TradeDatabase
                import requests as req
                db = TradeDatabase()
                positions = db.get_open_positions()
                db.close()
                if not positions:
                    return "No open positions"
                lines = []
                for p in positions[:5]:
                    try:
                        chain = p.get('chain','base')
                        token = p.get('token_address','')
                        entry = float(p.get('entry_price',0) or 0)
                        # live price
                        cur = entry
                        try:
                            r = req.get(f"https://api.dexscreener.com/tokens/v1/{chain}/{token}", timeout=5)
                            if r.status_code==200 and isinstance(r.json(), list) and r.json():
                                cur = float(r.json()[0].get('priceUsd',0) or entry)
                        except: pass
                        pnl = ((cur-entry)/entry*100) if entry else 0
                        x = cur/entry if entry else 1
                        addr = token
                        lines.append(f"{p.get('token_symbol','???')} <code>{addr[:6]}...{addr[-4:]}</code> {chain}\nEntry ${entry:.8f} -> ${cur:.8f} {pnl:+.2f}% x{x:.2f}\n<a href='https://dexscreener.com/{chain}/{token}'>dex</a> | <code>{token}</code>")
                    except Exception as e:
                        lines.append(str(e)[:100])
                return "\n\n".join(lines) + "\n\n<i>Tap address to copy</i>"
            elif cmd == "/scan":
                # tail logs
                from pathlib import Path
                p = Path("data/engine.log")
                tail = p.read_text(encoding='utf-8', errors='ignore').splitlines()[-8:] if p.exists() else []
                return "<b>Scan heartbeat (last 8):</b>\n" + "\n".join(f"<code>{l[-120:]}</code>" for l in tail[-8:])
            elif cmd == "/memory":
                mem = engine.memory.stats() if engine and engine.memory else {"total_memories":0}
                # lifecycles
                try:
                    lc = engine.lifecycle.stats() if engine and hasattr(engine,'lifecycle') and engine.lifecycle else {"total_lifecycles":0}
                except: lc = {"total_lifecycles":0}
                return f"🧠 Memory: {mem.get('total_memories',0)} trades win {mem.get('win_rate',0):.0%} | Lifecycles: {lc.get('total_lifecycles',0)}/10000 {'✅' if mem.get('ready_for_transformer') else '⏳'}"
            elif cmd == "/watched":
                lst = engine.alert_manager.list_watched() if engine and engine.alert_manager else []
                if not lst:
                    return "No watched wallets yet - lookup will populate after top 6h/24h scan"
                return "\n".join(f"{w['wallet'][:6]}...{w['wallet'][-4:]} {w['chain']} grade {w['grade']} via {w['coin_symbol']}" for w in lst[:10])
            elif cmd == "/alerts":
                lst = engine.alert_manager.list_alerts(limit=5) if engine and engine.alert_manager else []
                if not lst:
                    return "No alerts yet - watched wallets haven't traded new tokens"
                return "\n".join(f"🚨 {a['wallet'][:6]}... traded <code>{a['new_token'][:10]}...</code> {a['chain']} tx {a['tx_hash'][:8]}..." for a in lst)
            elif cmd == "/lookup":
                import threading, asyncio
                def _run():
                    from lookup.lookup import Lookup
                    from alerts.alert_manager import AlertManager
                    from utils.config import load_config
                    cfg = load_config()
                    lk = Lookup(cfg)
                    am = AlertManager(cfg)
                    lk.on_reputable = am.add_watch
                    asyncio.run(lk.run_once())
                threading.Thread(target=_run, daemon=True).start()
                return "Lookup triggered - brutal grading top 6h/24h coins, check /watched in 30s"
            elif cmd == "/logs":
                from pathlib import Path
                p = Path("data/engine.log")
                tail = p.read_text(encoding='utf-8', errors='ignore').splitlines()[-6:] if p.exists() else []
                return "<b>Logs tail:</b>\n" + "\n".join(f"<code>{l[-100:]}</code>" for l in tail[-6:])
            else:
                return f"Unknown {cmd} - try /help"
        except Exception as e:
            return f"⚠️ {e}"

    def _poll_loop(self):
        while self._running:
            try:
                for upd in self._get_updates():
                    self.offset = int(upd["update_id"]) + 1
                    try:
                        from pathlib import Path
                        Path("data/tg_offset.txt").write_text(str(self.offset), encoding='utf-8')
                    except: pass
                    msg = upd.get("message") or upd.get("edited_message")
                    if not msg: continue
                    text = msg.get("text", "")
                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    if text and chat_id:
                        if not self.chat_id or self.chat_id != chat_id:
                            self.chat_id = chat_id
                            self._save_chat(chat_id)
                        reply = self._handle(text, chat_id)
                        if reply:
                            self.send(chat_id, reply)
            except:
                time.sleep(2)

    def start(self):
        if not self.token or self.token.startswith("${"):
            print("[TG] no token - TG frontend disabled")
            return None
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        print(f"[TG] Meme Engine TG bot polling (paper $0.10)")
        return self._thread

    def stop(self):
        self._running = False

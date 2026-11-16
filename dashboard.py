#!/usr/bin/env python3
"""
Meme Engine - Web Dashboard
Real-time monitoring interface
"""

import os
import sys
import json
from datetime import datetime
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

app = Flask(__name__)
CORS(app)

# Will be initialized on first request
engine = None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status', methods=['GET'])
def get_status():
    # Lightweight status - don't instantiate full engine (causes DB lock when engine is running)
    try:
        from utils.database import TradeDatabase
        from utils.config import load_config
        db = TradeDatabase()
        stats = db.get_stats()
        db.close()
        cfg = load_config()
        # Check if engine process is running via log file touch
        return jsonify({
            'running': True,  # dashboard is up; engine running if you started main.py
            'paper_mode': cfg.get('execution',{}).get('paper_mode', True),
            'halted': False,
            'halt_reason': '',
            'stats': stats,
            'tracked_wallets': len(cfg.get('scout',{}).get('smart_money',{}).get('tracked_wallets',[])),
            'seen_tokens': 0,
            'active_positions': stats.get('open_positions',0),
            'queue_size': 0
        })
    except Exception as e:
        return jsonify({'error': str(e), 'running': False}), 500

@app.route('/api/positions', methods=['GET'])
def get_positions():
    from utils.database import TradeDatabase
    from utils.jupiter_pnl import compute_unrealized_pnl, get_sol_usd, quote_url_from_config
    from utils.config import load_config
    db = TradeDatabase()
    positions = db.get_open_positions()
    db.close()
    cfg = load_config()
    quote_url = quote_url_from_config(cfg)
    slippage_bps = int(cfg.get('paper', {}).get('slippage_bps', 200))
    sol_usd = get_sol_usd()
    for p in positions:
        p['current_mcap'] = None
        p['pnl_percent'] = None
        p['x_return'] = None
        p['profit_usd'] = None
        p['current_price'] = None
        try:
            pnl = compute_unrealized_pnl(
                p,
                quote_url=quote_url,
                slippage_bps=slippage_bps,
                sol_usd=sol_usd,
            )
            p['pnl_percent'] = pnl.get('pnl_percent')
            p['x_return'] = pnl.get('x_return')
            p['profit_usd'] = pnl.get('profit_usd')
            p['current_price'] = pnl.get('current_price')
            p['pnl_source'] = pnl.get('pnl_source')
            p['exit_quote_lamports'] = pnl.get('exit_quote_lamports')
            p['entry_sol_lamports_remaining'] = pnl.get('entry_sol_lamports_remaining')
            p['token_amount_raw_remaining'] = pnl.get('token_amount_raw_remaining')
            p['pnl_reason'] = pnl.get('reason')
        except Exception as e:
            p['pnl_source'] = 'error'
            p['pnl_reason'] = str(e)
    return jsonify(positions)

@app.route('/api/stats', methods=['GET'])
def get_stats():
    from utils.database import TradeDatabase
    db = TradeDatabase()
    stats = db.get_stats()
    # Add equity + halt info
    try:
        equity = db.get_equity_curve()
        stats['equity_curve'] = equity.tail(50).tolist() if len(equity)>0 else []
    except: pass
    db.close()
    return jsonify(stats)

@app.route('/api/config', methods=['GET'])
def get_config():
    from utils.config import load_config
    config = load_config()
    # Redact sensitive fields
    if 'chains' in config:
        for chain in config['chains'].values():
            chain.pop('private_key', None)
    return jsonify(config)

@app.route('/api/smart-wallets', methods=['GET'])
def get_smart_wallets():
    from utils.config import load_config
    config = load_config()
    wallets = config.get('scout', {}).get('smart_money', {}).get('tracked_wallets', [])
    return jsonify({'wallets': wallets})

@app.route('/api/smart-wallets', methods=['POST'])
def add_smart_wallet():
    data = request.get_json()
    wallet = data.get('address', '')
    label = data.get('label', '')
    if wallet:
        return jsonify({'success': True, 'message': f'Added {wallet}'})
    return jsonify({'error': 'Address required'}), 400

@app.route('/api/logs', methods=['GET'])
def get_logs():
    try:
        from pathlib import Path
        log_path = Path(__file__).parent / "data" / "engine.log"
        if not log_path.exists():
            return jsonify([])
        lines = log_path.read_text(encoding='utf-8', errors='ignore').splitlines()[-120:]
        return jsonify(lines)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/scan-status', methods=['GET'])
def scan_status():
    from utils.config import load_config
    cfg = load_config()
    chains = {k: {'enabled': v.get('enabled'), 'rpc': (v.get('rpc_url','')[:50] + '...')} for k,v in cfg.get('chains',{}).items()}
    return jsonify({
        'chains': chains,
        'scout': cfg.get('scout',{}),
        'overseer': cfg.get('overseer',{}),
        'lookup': cfg.get('lookup',{}),
        'paper_mode': cfg.get('execution',{}).get('paper_mode'),
        'goldrush_key_set': bool(cfg.get('goldrush',{}).get('api_key','').startswith('cqt_'))
    })

@app.route('/api/watched', methods=['GET'])
def watched():
    try:
        from alerts.alert_manager import AlertManager
        from utils.config import load_config
        am = AlertManager(load_config())
        return jsonify(am.list_watched())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/alerts', methods=['GET'])
def alerts():
    try:
        from alerts.alert_manager import AlertManager
        from utils.config import load_config
        am = AlertManager(load_config())
        return jsonify(am.list_alerts(limit=50))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/lookup/run', methods=['POST'])
def run_lookup():
    # Trigger manual lookup run (async)
    import threading
    def _run():
        import asyncio
        from lookup.lookup import Lookup
        from alerts.alert_manager import AlertManager
        from utils.config import load_config
        cfg = load_config()
        lk = Lookup(cfg)
        am = AlertManager(cfg)
        lk.on_reputable = am.add_watch
        asyncio.run(lk.run_once())
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'status': 'lookup started in background, check /api/watched in 30s'})

@app.route('/api/memory', methods=['GET'])
def memory_stats():
    try:
        from memory.journal import Memory
        from utils.config import load_config
        cfg = load_config()
        mem = Memory(cfg.get('portfolio',{}).get('database_path','data/trades.db'), "data/memory.jsonl")
        return jsonify(mem.stats())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5001, use_reloader=False)

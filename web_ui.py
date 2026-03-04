#!/usr/bin/env python3
"""
Web UI - Dashboard for analyzing recorded Polymarket data.
Reads JSONL files from snapshots/ and serves them via a web interface.

Usage:
    python web_ui.py [--port 5050] [--host 0.0.0.0]
"""

import os
import json
import argparse
from pathlib import Path
from flask import Flask, render_template, jsonify, request

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SNAPSHOTS_DIR = os.path.join(BASE_DIR, "snapshots")


def read_jsonl(filepath, limit=None):
    """Read a JSONL file and return list of dicts"""
    records = []
    if not os.path.exists(filepath):
        return records
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    except OSError:
        pass
    if limit and len(records) > limit:
        # Downsample evenly for large datasets, always include last element
        step = (len(records) - 1) / (limit - 1) if limit > 1 else 1
        sampled = [records[int(i * step)] for i in range(limit - 1)]
        sampled.append(records[-1])
        records = sampled
    return records


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/dates')
def api_dates():
    """List available snapshot dates"""
    dates = []
    if os.path.exists(SNAPSHOTS_DIR):
        for name in sorted(os.listdir(SNAPSHOTS_DIR), reverse=True):
            full = os.path.join(SNAPSHOTS_DIR, name)
            if os.path.isdir(full):
                # List what files exist in this date folder
                files = [f for f in os.listdir(full) if f.endswith('.jsonl')]
                dates.append({"date": name, "files": files})
    return jsonify(dates)


@app.route('/api/btc_prices')
def api_btc_prices():
    """BTC price data for a given date"""
    date = request.args.get('date')
    if not date:
        return jsonify({"error": "date parameter required"}), 400
    limit = request.args.get('limit', 5000, type=int)
    filepath = os.path.join(SNAPSHOTS_DIR, date, 'btc_prices.jsonl')
    data = read_jsonl(filepath, limit=limit)
    return jsonify(data)


@app.route('/api/market_snapshots')
def api_market_snapshots():
    """Market snapshot data (15m or 5m)"""
    date = request.args.get('date')
    market_type = request.args.get('type', '15m')
    if not date:
        return jsonify({"error": "date parameter required"}), 400
    if market_type not in ('15m', '5m'):
        return jsonify({"error": "type must be 15m or 5m"}), 400
    limit = request.args.get('limit', 5000, type=int)
    filepath = os.path.join(SNAPSHOTS_DIR, date, f'market_snapshots_{market_type}.jsonl')
    data = read_jsonl(filepath, limit=limit)
    return jsonify(data)


@app.route('/api/orderbook')
def api_orderbook():
    """Order book data (15m or 5m)"""
    date = request.args.get('date')
    market_type = request.args.get('type', '15m')
    if not date:
        return jsonify({"error": "date parameter required"}), 400
    if market_type not in ('15m', '5m'):
        return jsonify({"error": "type must be 15m or 5m"}), 400
    limit = request.args.get('limit', 2000, type=int)
    filepath = os.path.join(SNAPSHOTS_DIR, date, f'orderbook_{market_type}.jsonl')
    data = read_jsonl(filepath, limit=limit)
    return jsonify(data)


@app.route('/api/system_events')
def api_system_events():
    """System events for a given date"""
    date = request.args.get('date')
    if not date:
        return jsonify({"error": "date parameter required"}), 400
    filepath = os.path.join(SNAPSHOTS_DIR, date, 'system_events.jsonl')
    data = read_jsonl(filepath)
    return jsonify(data)


@app.route('/api/summary')
def api_summary():
    """Quick summary stats for a given date"""
    date = request.args.get('date')
    if not date:
        return jsonify({"error": "date parameter required"}), 400

    date_dir = os.path.join(SNAPSHOTS_DIR, date)
    if not os.path.isdir(date_dir):
        return jsonify({"error": "date not found"}), 404

    summary = {"date": date}

    # BTC price stats
    btc = read_jsonl(os.path.join(date_dir, 'btc_prices.jsonl'))
    if btc:
        prices = [r['binance_price'] for r in btc if r.get('binance_price')]
        lags = [r['lag_ms'] for r in btc if r.get('lag_ms') is not None]
        summary['btc'] = {
            'count': len(btc),
            'min': min(prices) if prices else None,
            'max': max(prices) if prices else None,
            'first': prices[0] if prices else None,
            'last': prices[-1] if prices else None,
            'avg_lag_ms': round(sum(lags) / len(lags)) if lags else None
        }

    # Market snapshot counts
    for mt in ['15m', '5m']:
        snaps = read_jsonl(os.path.join(date_dir, f'market_snapshots_{mt}.jsonl'))
        if snaps:
            slugs = set(r.get('market_slug', '') for r in snaps)
            summary[f'markets_{mt}'] = {
                'snapshot_count': len(snaps),
                'unique_markets': len(slugs)
            }

    # System events
    events = read_jsonl(os.path.join(date_dir, 'system_events.jsonl'))
    if events:
        summary['events'] = len(events)

    return jsonify(summary)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Polymarket Data Analysis Web UI')
    parser.add_argument('--port', type=int, default=5050, help='Port to run on (default: 5050)')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind to (default: 0.0.0.0)')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    args = parser.parse_args()

    print(f"\n📊 Polymarket Data Analysis UI")
    print(f"   http://localhost:{args.port}")
    print(f"   Snapshots dir: {SNAPSHOTS_DIR}\n")

    app.run(host=args.host, port=args.port, debug=args.debug)

#!/usr/bin/env python3
"""
Data Layer: WebSocket clients for real-time data.

Clients:
- BinanceClient: BTC price from Binance aggTrade
- CLOBClient: Polymarket orderbook
- RTDSClient: Polymarket oracle prices
- MarketDiscovery: Find active BTC 15m markets
"""

import asyncio
import aiohttp
import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Callable, List
from collections import deque

# Load environment variables
load_dotenv()


class BinanceClient:
    """Binance WebSocket for real-time BTC price"""
    
    def __init__(self, on_price_update: Callable[[float], None]):
        """
        Args:
            on_price_update: Callback function(price: float)
        """
        self.on_price_update = on_price_update
        self.ws_url = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
        self.running = False
        self.last_price = None
        
        # Connection health monitoring
        self.last_message_time = None
        self.connection_healthy = False
    
    async def start(self):
        """Start WebSocket connection (Direct connection for Binance)"""
        self.running = True
        
        while self.running:
            try:
                # Always use direct connection for Binance as requested
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.ws_url, heartbeat=15) as ws:
                        self.last_message_time = datetime.now().timestamp()
                        self.connection_healthy = True
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] Binance connected (direct)")
                        
                        while self.running:
                            try:
                                # Increase timeout for better stability
                                msg = await asyncio.wait_for(ws.receive(), timeout=60)
                            except asyncio.TimeoutError:
                                print(f"[{datetime.now().strftime('%H:%M:%S')}] [Binance] Timeout (60s), reconnecting...")
                                break

                            if not self.running:
                                break
                            
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self.last_message_time = datetime.now().timestamp()
                                self.connection_healthy = True
                                try:
                                    data = json.loads(msg.data)
                                    price = float(data.get('p', 0))
                                    
                                    if price > 0:
                                        self.last_price = price
                                        self.on_price_update(price)
                                except:
                                    continue
                            
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                print(f"[Binance] WebSocket closed or error")
                                break
                        
            except Exception as e:
                if self.running:
                    print(f"[Binance] Connection error: {e}, reconnecting in 5s...")
                    await asyncio.sleep(5)
    
    def stop(self):
        """Stop WebSocket"""
        self.running = False


class CLOBClient:
    """Polymarket CLOB WebSocket for orderbook"""
    
    def __init__(self, on_price_update: Callable[[str, Dict, Dict], None], 
                 on_market_resolved: Callable[[str, str], None] = None):
        self.on_price_update = on_price_update
        self.on_market_resolved = on_market_resolved
        self.ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        self.running = False
        
        # Multi-market support
        # markets: slug -> {'tokens': [up_id, down_id], 'prices': {...}}
        self.markets = {}
        self.token_to_slug = {} # asset_id -> slug
        
        self.need_resubscribe = False
        
        # Connection health monitoring
        self.last_message_time = None
        self.last_pong_time = None
        self.connection_healthy = False

    def set_market(self, token_ids: List[str], market_slug: str):
        """Add/Update market to subscribe to (doesn't start WS, just configures)"""
        # If already exists, do nothing unless tokens changed
        if market_slug in self.markets and self.markets[market_slug]['tokens'] == token_ids:
            return

        print(f"[CLOB] Adding market {market_slug}")
        self.markets[market_slug] = {
            'tokens': token_ids,
            'up_bid': None, 'up_ask': None,
            'down_bid': None, 'down_ask': None
        }
        
        # Update map
        for tid in token_ids:
            self.token_to_slug[tid] = market_slug
            
        self.need_resubscribe = True

    def remove_market(self, market_slug: str):
        """Stop tracking a market"""
        if market_slug in self.markets:
            print(f"[CLOB] Removing market {market_slug}")
            tokens = self.markets[market_slug]['tokens']
            for tid in tokens:
                if tid in self.token_to_slug:
                    del self.token_to_slug[tid]
            del self.markets[market_slug]
            self.need_resubscribe = True

    async def start(self):
        """Start WebSocket connection"""
        self.running = True
        
        while self.running:
            try:
                # Build asset list
                all_assets = list(self.token_to_slug.keys())
                if not all_assets:
                    await asyncio.sleep(1)
                    continue

                async with aiohttp.ClientSession() as session:
                    # Use aiohttp built-in heartbeat for protocol-level PING/PONG
                    # Polymarket CLOB requires stable connection, 20s heartbeat is optimal
                    async with session.ws_connect(self.ws_url, heartbeat=20, receive_timeout=60) as ws:
                        self.last_message_time = datetime.now().timestamp()
                        self.connection_healthy = True
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] CLOB connected ({len(self.markets)} markets) (direct)")
                        
                        subscribe_msg = {
                            "type": "market",
                            "assets_ids": all_assets,
                            "custom_feature_enabled": True
                        }
                        await ws.send_json(subscribe_msg)
                        self.need_resubscribe = False
                        
                        try:
                            while self.running:
                                if self.need_resubscribe:
                                    print(f"[{datetime.now().strftime('%H:%M:%S')}] [CLOB] Markets changed, reconnecting...")
                                    break
                                
                                try:
                                    msg = await ws.receive()
                                except asyncio.TimeoutError:
                                    # This should be handled by receive_timeout or heartbeat
                                    continue 

                                if not self.running: break
                                
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    self.last_message_time = datetime.now().timestamp()
                                    try:
                                        data = json.loads(msg.data)
                                        if isinstance(data, list):
                                            for item in data: self._parse_market_data(item)
                                        elif isinstance(data, dict):
                                            self._parse_market_data(data)
                                    except: continue
                                        
                                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    print(f"[CLOB] WebSocket {msg.type.name}")
                                    break
                        except Exception as e:
                            print(f"[CLOB] Connection error in loop: {e}")
                            break
                        
            except Exception as e:
                if self.running:
                    print(f"[CLOB] Error: {e}, reconnecting in 5s...")
                    await asyncio.sleep(5)

    def _parse_market_data(self, data: Dict):
        if not isinstance(data, dict): return
        
        # Check resolution (asset_id might be missing, try to find by winning ID?)
        # Polymarket 'market_resolved' event structure: {"event_type":"market_resolved", "market_slug": "...", ...}
        # OR it gives winning_asset_id.
        if data.get('event_type') == 'market_resolved':
            winning_asset_id = data.get('winning_asset_id')
            # Try to find slug from token_to_slug if asset_id provided, usually not in this event?
            # Data usually has asset_ids associated? 
            # If not, we iterate markets to find one that has this token.
            slug = None
            if winning_asset_id in self.token_to_slug:
                slug = self.token_to_slug[winning_asset_id]
            else:
                # Fallback: scan all markets
                for m_slug, m_data in self.markets.items():
                    if winning_asset_id in m_data['tokens']:
                        slug = m_slug
                        break
            
            if slug and self.on_market_resolved:
                print(f"[CLOB] Market resolved: {slug}")
                self.on_market_resolved(slug, winning_asset_id)
                self.remove_market(slug)
            return

        asset_id = data.get('asset_id')
        if not asset_id or asset_id not in self.token_to_slug:
            return
            
        slug = self.token_to_slug[asset_id]
        m_data = self.markets[slug]
        
        # Debug: Track event types (throttled to avoid spam) - DISABLED
        # if not hasattr(self, '_last_debug_time'):
        #     self._last_debug_time = {}
        # 
        # now_ts = datetime.now().timestamp()
        # if slug not in self._last_debug_time or (now_ts - self._last_debug_time.get(slug, 0)) > 10:
        #     event_type = "unknown"
        #     if 'best_bid' in data or 'best_ask' in data:
        #         event_type = "best_bid_ask"
        #     elif 'price' in data:
        #         event_type = "price_change"
        #     elif 'last_trade_price' in data:
        #         event_type = "last_trade"
        #     elif 'bids' in data and 'asks' in data:
        #         event_type = "orderbook"
        #     
        #     print(f"[CLOB DEBUG] {slug[:20]}: Event '{event_type}' (asset {asset_id[:8]}...)")
        #     self._last_debug_time[slug] = now_ts
        
        # Parse Price
        price_updated = False
        
        # PRIORITY 1: best_bid_ask event (real-time, requires custom_feature_enabled)
        if 'best_bid' in data or 'best_ask' in data:
            best_bid = data.get('best_bid')
            best_ask = data.get('best_ask')
            
            if best_bid or best_ask:
                if asset_id == m_data['tokens'][0]: # UP
                    if best_bid: m_data['up_bid'] = float(best_bid)
                    if best_ask: m_data['up_ask'] = float(best_ask)
                elif len(m_data['tokens']) > 1 and asset_id == m_data['tokens'][1]: # DOWN
                    if best_bid: m_data['down_bid'] = float(best_bid)
                    if best_ask: m_data['down_ask'] = float(best_ask)
                price_updated = True
        
        # PRIORITY 2: price_change event (sent on order updates)
        elif 'price' in data:
            price = float(data.get('price', 0))
            if price > 0:
                if asset_id == m_data['tokens'][0]: # UP
                    # Update mid price, keep spread from last known bid/ask
                    m_data['up_bid'] = price
                    m_data['up_ask'] = price
                elif len(m_data['tokens']) > 1 and asset_id == m_data['tokens'][1]: # DOWN
                    m_data['down_bid'] = price
                    m_data['down_ask'] = price
                price_updated = True
        
        # PRIORITY 3: Last Trade
        elif 'last_trade_price' in data:
            price = float(data.get('last_trade_price', 0))
            if price > 0:
                if asset_id == m_data['tokens'][0]: # UP
                    m_data['up_bid'] = price
                    m_data['up_ask'] = price
                elif len(m_data['tokens']) > 1 and asset_id == m_data['tokens'][1]: # DOWN
                    m_data['down_bid'] = price
                    m_data['down_ask'] = price
                price_updated = True

        # PRIORITY 4: Full Orderbook (snapshot or update)
        if not price_updated and 'bids' in data and 'asks' in data:
            bids = data.get('bids', [])
            asks = data.get('asks', [])
            
            best_bid = float(bids[-1]['price']) if bids and isinstance(bids[-1], dict) else (float(bids[-1][0]) if bids else None)
            best_ask = float(asks[-1]['price']) if asks and isinstance(asks[-1], dict) else (float(asks[-1][0]) if asks else None)
            
            if best_bid or best_ask:
                if asset_id == m_data['tokens'][0]:
                    if best_bid: m_data['up_bid'] = best_bid
                    if best_ask: m_data['up_ask'] = best_ask
                elif len(m_data['tokens']) > 1 and asset_id == m_data['tokens'][1]:
                    if best_bid: m_data['down_bid'] = best_bid
                    if best_ask: m_data['down_ask'] = best_ask
                price_updated = True

        if price_updated:
            self._send_update(slug)

    def _send_update(self, slug):
        m = self.markets[slug]
        
        # Check if we have UP prices
        up_prices = None
        if m['up_bid'] is not None and m['up_ask'] is not None:
            up_prices = {
                "bid": round(m['up_bid'], 4),
                "ask": round(m['up_ask'], 4),
                "mid": round((m['up_bid'] + m['up_ask']) / 2, 4)
            }
            
        # Check if we have DOWN prices
        down_prices = None
        if m['down_bid'] is not None and m['down_ask'] is not None:
            down_prices = {
                "bid": round(m['down_bid'], 4),
                "ask": round(m['down_ask'], 4),
                "mid": round((m['down_bid'] + m['down_ask']) / 2, 4)
            }
        
        # FALLBACK: If DOWN is missing but UP exists, calculate DOWN = 1 - UP
        # This is valid for binary markets where UP + DOWN = 1.0
        if up_prices and not down_prices:
            down_prices = {
                "bid": round(1.0 - m['up_ask'], 4),  # bid/ask are inverted
                "ask": round(1.0 - m['up_bid'], 4),
                "mid": round(1.0 - up_prices['mid'], 4)
            }
            # Update internal state for consistency
            m['down_bid'] = down_prices['bid']
            m['down_ask'] = down_prices['ask']
        
        # FALLBACK: If UP is missing but DOWN exists, calculate UP = 1 - DOWN
        elif down_prices and not up_prices:
            up_prices = {
                "bid": round(1.0 - m['down_ask'], 4),
                "ask": round(1.0 - m['down_bid'], 4),
                "mid": round(1.0 - down_prices['mid'], 4)
            }
            # Update internal state
            m['up_bid'] = up_prices['bid']
            m['up_ask'] = up_prices['ask']
            
        if up_prices or down_prices:
            self.on_price_update(slug, up_prices, down_prices)
    
    def stop(self):
        """Stop WebSocket"""
        self.running = False

    def get_market(self, market_slug: str) -> Optional[Dict]:
        """Fetch single market data via REST (CLOB first, then Gamma fallback)"""
        # 1. Try CLOB
        url = f"https://clob.polymarket.com/markets/{market_slug}"
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            print(f"[CLOB] get_market {market_slug} CLOB error: {e}")
            
        # 2. Try Gamma (Fallback for old/resolved markets)
        try:
            # Gamma usually returns list for ?slug=...
            g_url = f"https://gamma-api.polymarket.com/markets?slug={market_slug}"
            g_resp = requests.get(g_url, timeout=5)
            if g_resp.status_code == 200:
                data = g_resp.json()
                if isinstance(data, list) and data:
                    # Found in Gamma
                    return data[0]
        except Exception as e:
            print(f"[CLOB] get_market {market_slug} Gamma error: {e}")
            
        # If both failed
        print(f"[CLOB] get_market {market_slug} failed (Not found)")
        return None


class RTDSClient:
    """Polymarket RTDS WebSocket for oracle prices"""
    
    def __init__(self, on_oracle_update: Callable[[float], None]):
        """
        Args:
            on_oracle_update: Callback function(oracle_price: float)
        """
        self.on_oracle_update = on_oracle_update
        self.ws_url = "wss://ws-live-data.polymarket.com"
        self.running = False
        self.last_oracle_price = None
        
        # Price history for calculating momentum
        self.price_history = deque(maxlen=100000)  # Increased to guarantee 20+ minutes of history during high volatility
        
        # Connection health monitoring
        self.last_message_time = None
        self.connection_healthy = False
    
    async def start(self):
        """Start WebSocket connection"""
        self.running = True
        
        while self.running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.ws_url, heartbeat=20) as ws:
                        self.last_message_time = datetime.now().timestamp()
                        self.connection_healthy = True
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] RTDS connected (direct)")
                        
                        # Subscribe to BTC price (Binance source)
                        subscribe_msg = {
                            "action": "subscribe",
                            "subscriptions": [
                                {
                                    "topic": "crypto_prices",
                                    "type": "update",
                                    "filters": "btcusdt"
                                }
                            ]
                        }
                        await ws.send_json(subscribe_msg)
                        
                        # Also subscribe to Chainlink
                        subscribe_chainlink = {
                            "action": "subscribe",
                            "subscriptions": [
                                {
                                    "topic": "crypto_prices_chainlink",
                                    "type": "*",
                                    "filters": '{"symbol":"btc/usd"}'
                                }
                            ]
                        }
                        await ws.send_json(subscribe_chainlink)
                        
                        while self.running:
                            try:
                                msg = await asyncio.wait_for(ws.receive(), timeout=30)
                            except asyncio.TimeoutError:
                                print(f"[{datetime.now().strftime('%H:%M:%S')}] [RTDS] Timeout (30s), reconnecting...")
                                break

                            if not self.running:
                                break
                            
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self.last_message_time = datetime.now().timestamp()
                                self.connection_healthy = True
                                try:
                                    data = json.loads(msg.data)
                                    
                                    topic = data.get('topic', '')
                                    payload = data.get('payload', {})
                                    
                                    if 'crypto_prices' in topic:
                                        # Extract oracle price
                                        price = payload.get('value') or payload.get('price')
                                        
                                        if price:
                                            price = float(price)
                                            self.last_oracle_price = price
                                            
                                            # Add to history
                                            self.price_history.append({
                                                'time': datetime.now(timezone.utc),
                                                'price': price
                                            })
                                            
                                            self.on_oracle_update(price)
                                
                                except Exception as e:
                                    pass
                            
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                print(f"[RTDS] WebSocket error")
                                break
                        
            except Exception as e:
                if self.running:
                    print(f"[RTDS] Connection error: {e}, reconnecting in 5s...")
                    await asyncio.sleep(5)
    
    def get_price_change(self, seconds: int = 30) -> Optional[float]:
        """
        Calculate price change in USD over last N seconds.
        
        Returns:
            Price change in USD (positive = up, negative = down)
        """
        if len(self.price_history) < 2:
            return None
        
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=seconds)
        
        # Get prices within timeframe
        recent_prices = [p for p in self.price_history if p['time'] >= cutoff]
        
        if len(recent_prices) < 2:
            return None
        
        start_price = recent_prices[0]['price']
        end_price = recent_prices[-1]['price']
        
        return end_price - start_price
    
    def stop(self):
        """Stop WebSocket"""
        self.running = False


class MarketDiscovery:
    """Discover active BTC 15m markets"""
    
    @staticmethod
    def get_current_market() -> Optional[Dict]:
        """
        Get current active BTC 15m market.
        Returns market data with token_ids and end_date.
        """
        series_id = "10192"  # BTC 15m series
        
        try:
            response = requests.get(
                f"https://gamma-api.polymarket.com/series/{series_id}",
                timeout=10
            )
            
            if response.status_code == 200:
                serie_data = response.json()
                now = datetime.now(timezone.utc)
                
                # Find the NEXT upcoming market (ending after now)
                # Sort by end_time to get the closest one first
                upcoming_markets = []
                
                for event_summary in serie_data.get("events", []):
                    end_time_str = event_summary.get("endDate", "")
                    if not end_time_str:
                        continue
                    
                    end_time = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
                    
                    # Skip only already-expired or about-to-expire markets (< 10 seconds)
                    # This allows testing in the last minute but avoids selecting dead markets
                    MIN_BUFFER_SECONDS = 10
                    buffer_time = now + timedelta(seconds=MIN_BUFFER_SECONDS)
                    
                    if end_time > buffer_time:
                        upcoming_markets.append((end_time, event_summary))
                
                if not upcoming_markets:
                    # No active markets found
                    # print(f"[MarketDiscovery] No upcoming markets found! Current UTC: {now.strftime('%Y-%m-%d %H:%M:%S')}")
                    return None
                
                # Sort by end_time (earliest first = current active market)
                upcoming_markets.sort(key=lambda x: x[0])
                
                # Debug: Show what we found - DISABLED
                # print(f"[MarketDiscovery] Current UTC: {now.strftime('%Y-%m-%d %H:%M:%S')}")
                # print(f"[MarketDiscovery] Found {len(upcoming_markets)} upcoming market(s)")
                # for i, (end_time, ev) in enumerate(upcoming_markets[:3]):  # Show first 3
                #     mins_remaining = int((end_time - now).total_seconds() / 60)
                #     print(f"  [{i+1}] '{ev.get('title', 'N/A')[:30]}' ends at {end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC ({mins_remaining}m remaining)")
                
                # Select the FIRST one (earliest ending = current active market)
                selected_end_time, selected_event = upcoming_markets[0]
                # mins_remaining = int((selected_end_time - now).total_seconds() / 60)
                # print(f"[MarketDiscovery] Selecting market #1 (ends in {mins_remaining}m)")
                
                event_id = selected_event.get('id')
                event_resp = requests.get(
                    f"https://gamma-api.polymarket.com/events/{event_id}",
                    timeout=10
                )
                
                if event_resp.status_code == 200:
                    event_data = event_resp.json()
                    markets = event_data.get("markets", [])
                    
                    if markets:
                        m = markets[0]
                        clob_tokens = m.get("clobTokenIds", "[]")
                        
                        if isinstance(clob_tokens, str):
                            clob_tokens = json.loads(clob_tokens)
                        
                        # Get event start time for target price calculation
                        event_start_time = m.get("eventStartTime")
                        if event_start_time:
                            event_start_time = datetime.fromisoformat(event_start_time.replace("Z", "+00:00"))
                        
                        # Use the selected market's end time (from the event summary)
                        selected_end_date = selected_event.get("endDate", "")
                        
                        # Ensure UP (Yes) is first, DOWN (No) is second
                        # Typically outcomes are ["Yes", "No"] or ["Up", "Down"]
                        outcomes = m.get("outcomes", [])
                        if outcomes and len(outcomes) >= 2 and len(clob_tokens) >= 2:
                             # Check if first outcome is negative ("No", "Below", "Down")
                             first_outcome = str(outcomes[0]).lower()
                             if first_outcome in ["no", "below", "down"]:
                                 print(f"[MarketDiscovery] ⚠️ REVERSING token list because outcome[0] is '{outcomes[0]}'")
                                 clob_tokens = [clob_tokens[1], clob_tokens[0]]
                                 # outcomes is just checked, not returned
                        
                        return {
                                    "question": m.get("question"),
                                    "description": m.get("description"),
                                    "condition_id": m.get("conditionId"),
                                    "token_ids": clob_tokens,
                                    "end_date": selected_end_date,
                                    "end_time": selected_end_time,  # Use the datetime object from selection
                                    "slug": m.get("slug"),
                                    "event_start_time": event_start_time
                                }
        
        except Exception as e:
            pass  # print(f"[MarketDiscovery] Error: {e}")
        
        return None
    
    @staticmethod
    def _extract_target_price(market: Dict) -> Optional[float]:
        """
        Extract target/strike price from market description.
        The strike is usually in the market question like "Will BTC be above $96,500 at 4:15AM?"
        """
        import re
        
        # Try description first, then question
        text = market.get("description", "") or market.get("question", "")
        
        # Pattern to match dollar amounts like $96,500 or $96500.00
        patterns = [
            r'\$([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',  # $96,500.00
            r'\$([0-9]+(?:\.[0-9]+)?)',  # $96500
            r'above ([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',  # above 96,500
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                price_str = match.group(1).replace(',', '')
                try:
                    return float(price_str)
                except:
                    pass
        
        return None


class MarketDiscovery5m:
    """Discover active BTC 5m markets via slug generation"""

    @staticmethod
    def get_current_market() -> Optional[Dict]:
        """
        Get current active BTC 5m market.
        5m markets use slug format: btc-updown-5m-{start_timestamp}
        where start_timestamp is aligned to 300-second intervals.
        """
        interval = 300
        now_ts = int(datetime.now(timezone.utc).timestamp())
        current_bucket_start = now_ts - (now_ts % interval)

        # Check current bucket and next one
        candidates = [current_bucket_start, current_bucket_start + interval]

        for start_ts in candidates:
            slug = f"btc-updown-5m-{start_ts}"
            try:
                url = f"https://gamma-api.polymarket.com/events?slug={slug}"
                response = requests.get(url, timeout=5)

                if response.status_code == 200:
                    data = response.json()
                    if not data:
                        continue

                    event = data[0]
                    markets = event.get("markets", [])
                    if not markets:
                        continue

                    m = markets[0]

                    if m.get("closed") or m.get("resolved"):
                        continue

                    clob_tokens = m.get("clobTokenIds", "[]")
                    if isinstance(clob_tokens, str):
                        clob_tokens = json.loads(clob_tokens)

                    event_start_time = m.get("eventStartTime")
                    if event_start_time:
                        event_start_time = datetime.fromisoformat(
                            event_start_time.replace("Z", "+00:00")
                        )

                    end_time = datetime.fromtimestamp(start_ts + interval, timezone.utc)
                    end_date_str = m.get("endDate")

                    # Ensure UP is first, DOWN is second
                    outcomes = m.get("outcomes", [])
                    if outcomes and len(outcomes) >= 2 and len(clob_tokens) >= 2:
                        first_outcome = str(outcomes[0]).lower()
                        if first_outcome in ["no", "below", "down"]:
                            clob_tokens = [clob_tokens[1], clob_tokens[0]]

                    return {
                        "question": m.get("question"),
                        "description": m.get("description"),
                        "condition_id": m.get("conditionId"),
                        "token_ids": clob_tokens,
                        "end_date": end_date_str,
                        "end_time": end_time,
                        "slug": m.get("slug"),
                        "event_start_time": event_start_time,
                    }

            except Exception as e:
                pass  # Silently retry next candidate

        return None


if __name__ == "__main__":
    # Test data clients
    async def test_clients():
        print("Testing data clients...")
        
        # Test market discovery
        market = MarketDiscovery.get_current_market()
        if market:
            print(f"\n✓ Market found: {market['question'][:60]}")
        else:
            print("\n! No active market")
            return
        
        # Test Binance
        def on_binance_price(price):
            print(f"[Binance] ${price:,.2f}")
        
        binance = BinanceClient(on_binance_price)
        
        # Test CLOB
        def on_clob_price(slug, up, down):
            print(f"[CLOB] UP: ${up:.3f}, DOWN: ${down:.3f}")
        
        clob = CLOBClient(on_clob_price)
        clob.set_market(market['token_ids'], market['slug'])
        
        # Test RTDS
        def on_oracle_price(price):
            print(f"[RTDS] Oracle: ${price:,.2f}")
        
        rtds = RTDSClient(on_oracle_price)
        
        # Start all clients
        await asyncio.gather(
            binance.start(),
            clob.start(),
            rtds.start()
        )
    
    asyncio.run(test_clients())

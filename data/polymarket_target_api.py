"""
Polymarket Target Price API
Fetches strike price from Polymarket event page HTML
"""

import requests
import re
import json
from typing import Optional
import time
from datetime import datetime, timezone
from bs4 import BeautifulSoup

class PolymarketTargetPriceAPI:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
    
    def get_target_price(self, slug: str, max_retries: int = 3) -> Optional[float]:
        """
        Parse strike price from Polymarket event page HTML.
        Uses timestamp from slug and robust JSON traversal to find the correct openPrice.
        """
        # Extract timestamp from slug (e.g., btc-updown-15m-1769206500 -> 1769206500)
        ts_match = re.search(r'-(\d+)$', slug)
        target_ts_str = ts_match.group(1) if ts_match else None
        
        # Possible time formats in Polymarket JSON
        target_formats = []
        if target_ts_str:
            try:
                target_ts_int = int(target_ts_str)
                dt = datetime.fromtimestamp(target_ts_int, tz=timezone.utc)
                target_formats.append(dt.strftime('%Y-%m-%dT%H:%M:%S'))
                target_formats.append(dt.strftime('%Y-%m-%dT%H:%M:%SZ'))
                target_formats.append(dt.strftime('%Y-%m-%dT%H:%M:%S.000Z'))
                # Also include the raw timestamp as string
                target_formats.append(target_ts_str)
            except:
                pass

        for attempt in range(max_retries):
            try:
                url = f'https://polymarket.com/event/{slug}'
                response = self.session.get(url, timeout=10)
                
                if response.status_code != 200:
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    return None
                
                html = response.text
                
                # Method 1: Deep JSON Traversal
                try:
                    scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
                    for script in scripts:
                        if '"openPrice"' in script:
                            try:
                                json_match = re.search(r'(\{.*\})', script)
                                if json_match:
                                    data = json.loads(json_match.group(1))
                                    
                                    # State for recursive search
                                    found_prices = []

                                    def find_prices_recursive(obj, context=None):
                                        if isinstance(obj, dict):
                                            # If we find openPrice, record it with its context (the whole object)
                                            if 'openPrice' in obj and obj['openPrice']:
                                                found_prices.append(obj)
                                            
                                            for k, v in obj.items():
                                                find_prices_recursive(v, obj)
                                        elif isinstance(obj, list):
                                            for item in obj:
                                                find_prices_recursive(item, obj)

                                    find_prices_recursive(data)

                                    # Filter found prices by our slug or ticker
                                    for p_obj in found_prices:
                                        # 1. Direct Slug Match (Highest Priority)
                                        obj_slug = str(p_obj.get('slug', '')).lower()
                                        if obj_slug == slug.lower():
                                            return float(p_obj['openPrice'])
                                        
                                        # 2. Ticker match (often used in Polymarket JSON)
                                        obj_ticker = str(p_obj.get('ticker', '')).lower()
                                        if target_ts_str and target_ts_str in obj_ticker:
                                            return float(p_obj['openPrice'])

                                    # Method 2: Check dehydratedState queries for crypto-prices by timestamp
                                    # This is where "Price to beat" is usually stored
                                    try:
                                        queries = data.get('props', {}).get('pageProps', {}).get('dehydratedState', {}).get('queries', [])
                                        for q in queries:
                                            query_key = str(q.get('queryKey', ''))
                                            q_state = q.get('state', {})
                                            q_data = q_state.get('data', {})
                                            
                                            # Look for crypto-prices query matching our target timestamp
                                            is_crypto_price_query = 'crypto-prices' in query_key.lower()
                                            matches_timestamp = any(fmt.lower() in query_key.lower() for fmt in target_formats)
                                            
                                            if is_crypto_price_query and matches_timestamp:
                                                if isinstance(q_data, dict) and 'openPrice' in q_data:
                                                    return float(q_data['openPrice'])
                                            
                                            # Fallback: check slug in query data
                                            if isinstance(q_data, list):
                                                for item in q_data:
                                                    if isinstance(item, dict) and str(item.get('slug', '')).lower() == slug.lower():
                                                        if 'openPrice' in item: return float(item['openPrice'])
                                            elif isinstance(q_data, dict):
                                                if str(q_data.get('slug', '')).lower() == slug.lower():
                                                    if 'openPrice' in q_data: return float(q_data['openPrice'])
                                    except:
                                        pass
                            except:
                                pass
                except Exception:
                    pass

                # Method 2: High-accuracy regex (slug-anchored)
                # Matches: "slug":"...slug...","openPrice":89123.45
                price_match = re.search(fr'"{re.escape(slug)}".*?"openPrice":\s*([\d.]+)', html)
                if price_match:
                    return float(price_match.group(1))
                
                # Method 3: Legacy Regex/BeautifulSoup for "Price to beat" (as last resort)
                # ... (rest of the code remains as fallback)
                # We try to find "Price to beat" or "Strike price"
                patterns = [
                    r'Price to beat[^\$]*\$\s*([\d,]+\.?\d*)',
                    r'price to beat[^\$]*\$\s*([\d,]+\.?\d*)',
                    r'Strike price[^\$]*\$\s*([\d,]+\.?\d*)',
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
                    if match:
                        price_str = match.group(1).replace(',', '')
                        try:
                            price = float(price_str)
                            if price > 0 and price < 1000000:
                                return price
                        except ValueError:
                            continue
                
                # Method 3: BeautifulSoup parsing for "Price to beat"
                try:
                    soup = BeautifulSoup(html, 'html.parser')
                    for element in soup.find_all(string=re.compile(r'Price to beat', re.IGNORECASE)):
                        parent_text = element.parent.get_text() if element.parent else str(element)
                        match = re.search(r'\$\s*([\d,]+\.?\d*)', parent_text)
                        if match:
                            price_str = match.group(1).replace(',', '')
                            try:
                                price = float(price_str)
                                if price > 0:
                                    return price
                            except ValueError:
                                continue
                except Exception:
                    pass
                
                if attempt < max_retries - 1:
                    time.sleep(2)
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
        
        return None

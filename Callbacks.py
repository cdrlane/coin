"""
CoinEx WebSocket Custom Callback Example - FIXED VERSION

This example shows how to use custom callbacks to handle different
types of WebSocket messages for more complex applications.

This is a standalone version with the WebSocket client included.
"""

import asyncio
import json
import websockets
import hashlib
import hmac
import time
import gzip
from typing import Optional, Dict, Callable


class CoinExWebSocket:
    """CoinEx WebSocket API client with authentication"""
    
    def __init__(self, access_id: Optional[str] = None, secret_key: Optional[str] = None):
        self.access_id = access_id
        self.secret_key = secret_key
        self.ws_url = "wss://socket.coinex.com/v2/spot"
        self.websocket = None
        self.authenticated = False
        
    def _generate_signature(self, timestamp: int) -> str:
        message = str(timestamp)
        signature = hmac.new(
            self.secret_key.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    async def connect(self):
        """Establish WebSocket connection with deflate compression"""
        print("Connecting to CoinEx WebSocket...")
        # CoinEx v2 requires deflate compression
        self.websocket = await websockets.connect(
            self.ws_url,
            compression='deflate'
        )
        print("Connected successfully!")
        
    async def authenticate(self):
        if not self.access_id or not self.secret_key:
            raise ValueError("API credentials required for authentication")
        
        timestamp = int(time.time() * 1000)
        signature = self._generate_signature(timestamp)
        
        auth_message = {
            "method": "server.sign",
            "params": {
                "access_id": self.access_id,
                "timestamp": timestamp,
                "signature": signature
            },
            "id": 1
        }
        
        print("Authenticating...")
        await self.websocket.send(json.dumps(auth_message))
        
        response = await self.websocket.recv()
        result = json.loads(response)
        
        if result.get("error") is None and result.get("result", {}).get("status") == "success":
            self.authenticated = True
            print("✓ Authentication successful!")
        else:
            print(f"✗ Authentication failed: {result}")
            raise Exception("Authentication failed")
    
    async def subscribe_ticker(self, market: str):
        subscribe_message = {
            "method": "state.subscribe",
            "params": {
                "market_list": [market]
            },
            "id": 2
        }
        await self.websocket.send(json.dumps(subscribe_message))
    
    async def subscribe_depth(self, market: str, limit: int = 20, interval: str = "0"):
        subscribe_message = {
            "method": "depth.subscribe",
            "params": {
                "market_list": [market],
                "limit": limit,
                "interval": interval
            },
            "id": 3
        }
        await self.websocket.send(json.dumps(subscribe_message))
    
    async def subscribe_trades(self, market: str):
        subscribe_message = {
            "method": "deals.subscribe",
            "params": {
                "market_list": [market]
            },
            "id": 4
        }
        await self.websocket.send(json.dumps(subscribe_message))
    
    async def ping(self):
        ping_message = {
            "method": "server.ping",
            "params": {},
            "id": 100
        }
        await self.websocket.send(json.dumps(ping_message))
    
    async def listen(self, callback: Optional[Callable] = None):
        """Listen for messages with improved error handling and compression support"""
        try:
            while True:
                message = await self.websocket.recv()
                
                # Handle compression - CoinEx sends gzip despite deflate connection
                if isinstance(message, bytes):
                    # Check if it's gzip compressed (starts with 0x1f 0x8b)
                    if len(message) > 1 and message[0] == 0x1f and message[1] == 0x8b:
                        try:
                            message = gzip.decompress(message).decode('utf-8')
                        except Exception as e:
                            print(f"⚠️  Error decompressing message: {e}")
                            continue
                    else:
                        try:
                            message = message.decode('utf-8')
                        except Exception as e:
                            print(f"⚠️  Error decoding message: {e}")
                            continue
                
                # Parse JSON
                try:
                    data = json.loads(message)
                except json.JSONDecodeError as e:
                    print(f"⚠️  Error parsing JSON: {e}")
                    print(f"   Raw message: {message[:200]}")
                    continue
                
                if callback:
                    try:
                        callback(data)
                    except Exception as e:
                        print(f"⚠️  Error in callback: {e}")
                        import traceback
                        traceback.print_exc()
                        
        except websockets.exceptions.ConnectionClosed:
            print("Connection closed")
        except Exception as e:
            print(f"Error in listen: {e}")
            import traceback
            traceback.print_exc()
    
    async def close(self):
        if self.websocket:
            await self.websocket.close()
            print("Connection closed")


class TradingBot:
    """Example trading bot that processes WebSocket data"""
    
    def __init__(self, verbose=True):
        self.last_price = None
        self.order_book = {"asks": [], "bids": []}
        self.recent_trades = []
        self.verbose = verbose  # Control detailed vs simple output
        self.price_history = []  # Track price history
        
    def handle_ticker(self, data):
        """Handle ticker updates with error checking and display last traded price"""
        try:
            # CoinEx v2 uses data.state_list, not params
            state_list = data.get("data", {}).get("state_list", [])
            if not state_list or not isinstance(state_list, list):
                return
                
            ticker = state_list[0]
            market = ticker.get("market", "Unknown")
            last_price_str = ticker.get("last", "0")
            
            # Handle both string and numeric values
            try:
                last_price = float(last_price_str)
            except (ValueError, TypeError):
                print(f"⚠️  Invalid price format: {last_price_str}")
                return
            
            # Get additional ticker data
            volume = ticker.get("volume", "0")
            open_price_str = ticker.get("open", "0")
            high_24h = ticker.get("high", "0")
            low_24h = ticker.get("low", "0")
            
            # Calculate 24h change
            try:
                open_price = float(open_price_str)
                if open_price > 0:
                    change_24h = ((last_price - open_price) / open_price) * 100
                else:
                    change_24h = 0
            except (ValueError, TypeError):
                change_24h = 0
            
            # Add to price history
            self.price_history.append(last_price)
            if len(self.price_history) > 100:
                self.price_history.pop(0)
            
            if self.verbose:
                # Detailed display
                print(f"\n{'='*60}")
                print(f"💰 LAST TRADED PRICE - {market}")
                print(f"{'='*60}")
                print(f"   Price:     ${last_price:,.2f}")
                
                # Show change from previous update if available
                if self.last_price and self.last_price > 0:
                    change = ((last_price - self.last_price) / self.last_price) * 100
                    direction = "📈" if change > 0 else "📉"
                    print(f"   Change:    {direction} {change:+.2f}% (from last update)")
                
                # Show 24h statistics
                try:
                    print(f"   24h Change: {change_24h:+.2f}%")
                    print(f"   24h High:   ${float(high_24h):,.2f}")
                    print(f"   24h Low:    ${float(low_24h):,.2f}")
                    print(f"   24h Volume: {float(volume):,.2f}")
                except (ValueError, TypeError):
                    pass
                
                print(f"{'='*60}\n")
            else:
                # Simple compact display
                timestamp = time.strftime("%H:%M:%S")
                change_indicator = ""
                if self.last_price and self.last_price > 0:
                    change = ((last_price - self.last_price) / self.last_price) * 100
                    if abs(change) > 0.001:  # Only show if changed
                        direction = "▲" if change > 0 else "▼"
                        change_indicator = f" {direction} {abs(change):.2f}%"
                
                print(f"[{timestamp}] 💰 {market}: ${last_price:,.2f}{change_indicator}")
                
            self.last_price = last_price
            
        except Exception as e:
            print(f"⚠️  Error handling ticker: {e}")
    
    def handle_depth(self, data):
        """Handle order book depth updates with error checking"""
        try:
            # CoinEx v2 uses data structure, not params
            depth_data = data.get("data", {})
            if not isinstance(depth_data, dict):
                return
            
            market = depth_data.get("market", "Unknown")
            
            self.order_book["asks"] = depth_data.get("asks", [])
            self.order_book["bids"] = depth_data.get("bids", [])
            
            if self.order_book["asks"] and self.order_book["bids"]:
                try:
                    best_ask = float(self.order_book["asks"][0][0])
                    best_bid = float(self.order_book["bids"][0][0])
                    spread = best_ask - best_bid
                    spread_pct = (spread / best_bid) * 100
                    
                    print(f"📖 {market} - Spread: ${spread:.2f} ({spread_pct:.3f}%)")
                    print(f"   Ask: ${best_ask:,.2f} | Bid: ${best_bid:,.2f}")
                except (ValueError, TypeError, IndexError) as e:
                    print(f"⚠️  Error parsing order book: {e}")
                    
        except Exception as e:
            print(f"⚠️  Error handling depth: {e}")
    
    def handle_trades(self, data):
        """Handle trade updates with error checking"""
        try:
            # CoinEx v2 uses data structure, not params
            deals_data = data.get("data", {})
            if not isinstance(deals_data, dict):
                return
            
            market = deals_data.get("market", "Unknown")
            deals = deals_data.get("deals", [])
            
            if not isinstance(deals, list):
                return
            
            for trade in deals:
                try:
                    price = float(trade.get("price", 0))
                    amount = float(trade.get("amount", 0))
                    trade_type = trade.get("type", "unknown")
                    volume = price * amount
                    
                    emoji = "🟢" if trade_type == "buy" else "🔴"
                    print(f"{emoji} {market} {trade_type.upper()}: {amount:.6f} @ ${price:,.2f} (${volume:,.2f})")
                    
                    self.recent_trades.append(trade)
                    # Keep only last 10 trades
                    if len(self.recent_trades) > 10:
                        self.recent_trades.pop(0)
                        
                except (ValueError, TypeError) as e:
                    print(f"⚠️  Error parsing trade: {e}")
                    
        except Exception as e:
            print(f"⚠️  Error handling trades: {e}")
    
    def handle_message(self, data):
        """Main message handler that routes to specific handlers"""
        try:
            method = data.get("method", "")
            
            if method == "state.update":
                self.handle_ticker(data)
            elif method == "depth.update":
                self.handle_depth(data)
            elif method == "deals.update":
                self.handle_trades(data)
            elif method == "server.pong":
                pass  # Ignore pong messages
            else:
                # Handle subscription confirmations and other messages
                if "result" in data:
                    result = data.get("result", {})
                    if result.get("status") == "success":
                        print(f"✓ Subscription successful")
                elif "error" in data:
                    print(f"⚠️  Error: {data.get('error')}")
                    
        except Exception as e:
            print(f"⚠️  Error in message handler: {e}")
            print(f"   Message was: {json.dumps(data, indent=2)}")


async def example_with_bot():
    """Example using a custom bot with callbacks"""
    print("=" * 60)
    print("Trading Bot with Custom Callbacks")
    print("=" * 60 + "\n")
    
    # Create bot instance
    bot = TradingBot()
    
    # Create WebSocket client
    client = CoinExWebSocket()
    
    try:
        # Connect
        await client.connect()
        
        # Wait a moment for connection to establish
        await asyncio.sleep(1)
        
        # Subscribe to multiple markets
        markets = ["BTCUSDT", "ETHUSDT"]
        
        for market in markets:
            await client.subscribe_ticker(market)
            await asyncio.sleep(0.5)  # Small delay between subscriptions
            
            await client.subscribe_depth(market, limit=5)
            await asyncio.sleep(0.5)
            
            await client.subscribe_trades(market)
            await asyncio.sleep(0.5)
            
            print(f"✓ Subscribed to {market}")
        
        print("\n" + "-" * 60)
        print("Monitoring markets... (Press Ctrl+C to stop)")
        print("-" * 60 + "\n")
        
        # Listen with custom callback
        await client.listen(callback=bot.handle_message)
        
    except KeyboardInterrupt:
        print("\n\nShutting down...")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.close()


async def example_price_alert():
    """Example: Simple price alert system"""
    print("=" * 60)
    print("Price Alert System")
    print("=" * 60 + "\n")
    
    # Set price thresholds (adjust these based on current BTC price)
    ALERT_HIGH = 105000.00
    ALERT_LOW = 95000.00
    
    alerted_high = False
    alerted_low = False
    
    def price_alert_handler(data):
        nonlocal alerted_high, alerted_low
        
        try:
            method = data.get("method", "")
            if method == "state.update":
                params = data.get("params", [])
                if params and isinstance(params, list):
                    ticker = params[0]
                    market = ticker.get("market", "")
                    
                    try:
                        price = float(ticker.get("last", 0))
                    except (ValueError, TypeError):
                        return
                    
                    print(f"Current {market} price: ${price:,.2f}")
                    
                    # Check alerts
                    if price >= ALERT_HIGH and not alerted_high:
                        print(f"\n🚨 ALERT! {market} reached ${price:,.2f} (above ${ALERT_HIGH:,.2f})")
                        alerted_high = True
                    elif price <= ALERT_LOW and not alerted_low:
                        print(f"\n🚨 ALERT! {market} dropped to ${price:,.2f} (below ${ALERT_LOW:,.2f})")
                        alerted_low = True
                    
                    # Reset alerts if price moves back
                    if ALERT_LOW < price < ALERT_HIGH:
                        alerted_high = False
                        alerted_low = False
                        
        except Exception as e:
            print(f"⚠️  Error in price alert handler: {e}")
    
    client = CoinExWebSocket()
    
    try:
        await client.connect()
        await asyncio.sleep(1)
        
        await client.subscribe_ticker("BTCUSDT")
        
        print(f"Alert thresholds:")
        print(f"  High: ${ALERT_HIGH:,.2f}")
        print(f"  Low:  ${ALERT_LOW:,.2f}")
        print("\nMonitoring price...\n")
        
        await asyncio.wait_for(
            client.listen(callback=price_alert_handler),
            timeout=300.0  # Run for 5 minutes
        )
        
    except asyncio.TimeoutError:
        print("\nMonitoring period ended")
    except KeyboardInterrupt:
        print("\n\nStopped by user")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.close()


async def example_simple_price_monitor():
    """Example: Simple continuous price monitoring"""
    print("=" * 60)
    print("Simple Price Monitor - Last Traded Prices")
    print("=" * 60 + "\n")
    
    # Create bot with simple output mode
    bot = TradingBot(verbose=False)
    
    # Create WebSocket client
    client = CoinExWebSocket()
    
    try:
        await client.connect()
        await asyncio.sleep(1)
        
        # Subscribe to ticker only
        markets = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        
        for market in markets:
            await client.subscribe_ticker(market)
            await asyncio.sleep(0.5)
            print(f"✓ Monitoring {market}")
        
        print("\n" + "-" * 60)
        print("Displaying last traded prices (Press Ctrl+C to stop)")
        print("-" * 60 + "\n")
        
        # Listen with custom callback
        await client.listen(callback=bot.handle_message)
        
    except KeyboardInterrupt:
        print("\n\nShutting down...")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.close()


async def main():
    """Run examples"""
    
    print("Select example to run:")
    print("1. Simple Price Monitor (clean, continuous price display)")
    print("2. Trading Bot with Full Details (verbose output)")
    print("3. Price Alert System")
    print()
    
    # For automatic running, use simple price monitor
    # Change this to run different examples
    
    # Example 1: Simple price monitoring (recommended)
    await example_simple_price_monitor()
    
    # Example 2: Trading bot with detailed callbacks
    # Uncomment to run:
    # await example_with_bot()
    
    # Example 3: Price alert system
    # Uncomment to run:
    # await example_price_alert()


if __name__ == "__main__":
    asyncio.run(main())
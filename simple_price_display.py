"""
CoinEx WebSocket - Simple Last Price Display

A clean, simple display of last traded prices for multiple markets.
Perfect for monitoring prices in real-time.
"""

import asyncio
import json
import websockets
import hashlib
import hmac
import time
import gzip
from typing import Optional
from datetime import datetime


class SimplePriceMonitor:
    """Simple WebSocket client for displaying last traded prices"""
    
    def __init__(self):
        self.ws_url = "wss://socket.coinex.com/v2/spot"
        self.websocket = None
        self.prices = {}  # Store current prices for each market
        
    async def connect(self):
        """Connect to CoinEx WebSocket with deflate compression"""
        print("Connecting to CoinEx...")
        # CoinEx v2 requires deflate compression
        self.websocket = await websockets.connect(
            self.ws_url,
            compression='deflate'
        )
        print("✓ Connected!\n")
        
    async def subscribe_ticker(self, market: str):
        """Subscribe to ticker updates for a market"""
        subscribe_message = {
            "method": "state.subscribe",
            "params": {
                "market_list": [market]
            },
            "id": int(time.time())
        }
        await self.websocket.send(json.dumps(subscribe_message))
        
    async def listen(self):
        """Listen for price updates and display them"""
        try:
            while True:
                message = await self.websocket.recv()
                
                # Handle compression - CoinEx may send gzip despite deflate connection
                if isinstance(message, bytes):
                    # Check if it's gzip compressed (starts with 0x1f 0x8b)
                    if len(message) > 1 and message[0] == 0x1f and message[1] == 0x8b:
                        try:
                            message = gzip.decompress(message).decode('utf-8')
                        except Exception as e:
                            print(f"⚠️  Gzip decompression error: {e}")
                            continue
                    else:
                        # Try regular decode
                        try:
                            message = message.decode('utf-8')
                        except Exception as e:
                            print(f"⚠️  Decode error: {e}")
                            continue
                
                # Parse JSON
                try:
                    data = json.loads(message)
                except Exception as e:
                    print(f"⚠️  JSON parse error: {e}")
                    continue
                
                # Handle ticker updates
                if data.get("method") == "state.update":
                    self.handle_price_update(data)
                    
        except websockets.exceptions.ConnectionClosed:
            print("\n❌ Connection closed")
        except KeyboardInterrupt:
            print("\n\n👋 Shutting down...")
        except Exception as e:
            print(f"\n❌ Error: {e}")
    
    def handle_price_update(self, data):
        """Handle and display price updates"""
        try:
            # CoinEx v2 uses data.state_list, not params
            state_list = data.get("data", {}).get("state_list", [])
            if not state_list:
                return
            
            ticker = state_list[0]
            market = ticker.get("market", "")
            
            # Parse price data
            try:
                last = float(ticker.get("last", 0))
                volume = float(ticker.get("volume", 0))
                open_price = float(ticker.get("open", 0))
                high = float(ticker.get("high", 0))
                low = float(ticker.get("low", 0))
                
                # Calculate 24h change percentage
                if open_price > 0:
                    change_24h = ((last - open_price) / open_price) * 100
                else:
                    change_24h = 0
                    
            except (ValueError, TypeError):
                return
            
            # Get previous price for this market
            prev_price = self.prices.get(market, last)
            
            # Determine if price went up or down
            if last > prev_price:
                trend = "📈"
                change_color = "\033[92m"  # Green
            elif last < prev_price:
                trend = "📉"
                change_color = "\033[91m"  # Red
            else:
                trend = "━"
                change_color = "\033[93m"  # Yellow
            
            reset_color = "\033[0m"
            
            # Update stored price
            self.prices[market] = last
            
            # Get current time
            timestamp = datetime.now().strftime("%H:%M:%S")
            
            # Display the price update
            print(f"{trend} [{timestamp}] {market:12s} | "
                  f"{change_color}${last:>12,.2f}{reset_color} | "
                  f"24h: {change_24h:>+6.2f}% | "
                  f"Vol: {volume:>12,.2f}")
            
        except Exception as e:
            pass  # Silently ignore errors to keep display clean
    
    async def close(self):
        """Close the WebSocket connection"""
        if self.websocket:
            await self.websocket.close()


async def main():
    """Main function to run the price monitor"""
    
    # Configure markets to monitor
    markets = [
        "BTCUSDT",   # Bitcoin
        "ETHUSDT",   # Ethereum
        "BNBUSDT",   # Binance Coin
        "SOLUSDT",   # Solana
        "XRPUSDT",   # Ripple
        "ADAUSDT",   # Cardano
        "DOGEUSDT",  # Dogecoin
        "TRXUSDT",   # Tron
    ]
    
    monitor = SimplePriceMonitor()
    
    try:
        # Connect
        await monitor.connect()
        
        # Subscribe to all markets
        print("Subscribing to markets...")
        for market in markets:
            await monitor.subscribe_ticker(market)
            await asyncio.sleep(0.3)
        
        print(f"✓ Monitoring {len(markets)} markets\n")
        print("=" * 90)
        print(f"{'TREND':<3} {'TIME':<10} {'MARKET':<12} | {'LAST PRICE':>14} | "
              f"{'24H CHANGE':>10} | {'VOLUME':>14}")
        print("=" * 90)
        
        # Listen for updates
        await monitor.listen()
        
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await monitor.close()


if __name__ == "__main__":
    # Add some styling
    print("\n" + "=" * 90)
    print(" " * 30 + "📊 COINEX PRICE MONITOR 📊")
    print("=" * 90 + "\n")
    
    asyncio.run(main())
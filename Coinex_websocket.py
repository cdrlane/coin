"""
CoinEx WebSocket API Authentication Example

This script demonstrates how to connect to and authenticate with the CoinEx WebSocket API.
The WebSocket API provides real-time market data and allows authenticated trading operations.

You'll need to:
1. Create an account on CoinEx
2. Generate API keys from your account settings
3. Keep your API secret secure (never commit it to version control)
"""

import asyncio
import hashlib
import hmac
import json
import time
import gzip
import websockets
from typing import Optional, Dict, Callable


class CoinExWebSocket:
    """CoinEx WebSocket API client with authentication"""
    
    def __init__(self, access_id: Optional[str] = None, secret_key: Optional[str] = None):
        """
        Initialize the CoinEx WebSocket client
        
        Args:
            access_id: Your CoinEx API access ID (optional for public data)
            secret_key: Your CoinEx API secret key (optional for public data)
        """
        self.access_id = access_id
        self.secret_key = secret_key
        self.ws_url = "wss://socket.coinex.com/v2/spot"
        self.websocket = None
        self.callbacks = {}
        self.authenticated = False
        
    def _generate_signature(self, timestamp: int) -> str:
        """
        Generate signature for WebSocket authentication
        
        Args:
            timestamp: Unix timestamp in milliseconds
            
        Returns:
            Hexadecimal signature string
        """
        # Create the message to sign: timestamp
        message = str(timestamp)
        
        # Generate HMAC-SHA256 signature
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
        """
        Authenticate the WebSocket connection
        Requires access_id and secret_key to be set
        """
        if not self.access_id or not self.secret_key:
            raise ValueError("API credentials required for authentication")
        
        # Generate timestamp in milliseconds
        timestamp = int(time.time() * 1000)
        
        # Generate signature
        signature = self._generate_signature(timestamp)
        
        # Prepare authentication message
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
        
        # Wait for authentication response
        response = await self.websocket.recv()
        result = json.loads(response)
        
        if result.get("error") is None and result.get("result", {}).get("status") == "success":
            self.authenticated = True
            print("✓ Authentication successful!")
        else:
            print(f"✗ Authentication failed: {result}")
            raise Exception("Authentication failed")
    
    async def subscribe_ticker(self, market: str):
        """
        Subscribe to real-time ticker updates for a market
        
        Args:
            market: Market pair (e.g., 'BTCUSDT')
        """
        subscribe_message = {
            "method": "state.subscribe",
            "params": {
                "market_list": [market]
            },
            "id": 2
        }
        
        print(f"Subscribing to ticker for {market}...")
        await self.websocket.send(json.dumps(subscribe_message))
    
    async def subscribe_depth(self, market: str, limit: int = 20, interval: str = "0"):
        """
        Subscribe to order book depth updates
        
        Args:
            market: Market pair (e.g., 'BTCUSDT')
            limit: Depth limit (5, 10, 20, 50, 100)
            interval: Update interval ("0" for real-time, "0.1", "0.2", etc.)
        """
        subscribe_message = {
            "method": "depth.subscribe",
            "params": {
                "market_list": [market],
                "limit": limit,
                "interval": interval
            },
            "id": 3
        }
        
        print(f"Subscribing to order book depth for {market}...")
        await self.websocket.send(json.dumps(subscribe_message))
    
    async def subscribe_trades(self, market: str):
        """
        Subscribe to real-time trade updates
        
        Args:
            market: Market pair (e.g., 'BTCUSDT')
        """
        subscribe_message = {
            "method": "deals.subscribe",
            "params": {
                "market_list": [market]
            },
            "id": 4
        }
        
        print(f"Subscribing to trades for {market}...")
        await self.websocket.send(json.dumps(subscribe_message))
    
    async def subscribe_user_deals(self):
        """
        Subscribe to user's trade updates (requires authentication)
        """
        if not self.authenticated:
            raise Exception("Authentication required for user deals subscription")
        
        subscribe_message = {
            "method": "user_deals.subscribe",
            "params": {},
            "id": 5
        }
        
        print("Subscribing to user deals...")
        await self.websocket.send(json.dumps(subscribe_message))
    
    async def subscribe_user_order(self):
        """
        Subscribe to user's order updates (requires authentication)
        """
        if not self.authenticated:
            raise Exception("Authentication required for user order subscription")
        
        subscribe_message = {
            "method": "order.subscribe",
            "params": {},
            "id": 6
        }
        
        print("Subscribing to user orders...")
        await self.websocket.send(json.dumps(subscribe_message))
    
    async def subscribe_balance(self):
        """
        Subscribe to balance updates (requires authentication)
        """
        if not self.authenticated:
            raise Exception("Authentication required for balance subscription")
        
        subscribe_message = {
            "method": "asset.subscribe",
            "params": {},
            "id": 7
        }
        
        print("Subscribing to balance updates...")
        await self.websocket.send(json.dumps(subscribe_message))
    
    async def ping(self):
        """Send ping to keep connection alive"""
        ping_message = {
            "method": "server.ping",
            "params": {},
            "id": 100
        }
        await self.websocket.send(json.dumps(ping_message))
    
    async def listen(self, callback: Optional[Callable] = None):
        """
        Listen for messages from the WebSocket
        
        Args:
            callback: Optional callback function to handle messages
        """
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
                            print(f"Error decompressing message: {e}")
                            continue
                    else:
                        try:
                            message = message.decode('utf-8')
                        except Exception as e:
                            print(f"Error decoding message: {e}")
                            continue
                
                # Parse JSON
                try:
                    data = json.loads(message)
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON: {e}")
                    continue
                
                if callback:
                    callback(data)
                else:
                    self._default_message_handler(data)
                    
        except websockets.exceptions.ConnectionClosed:
            print("Connection closed")
        except Exception as e:
            print(f"Error in listen: {e}")
    
    def _default_message_handler(self, data: Dict):
        """Default message handler that prints received data"""
        method = data.get("method", "")
        
        if method == "state.update":
            # Ticker update - CoinEx v2 uses data.state_list
            state_list = data.get("data", {}).get("state_list", [])
            if state_list:
                ticker = state_list[0]
                market = ticker.get("market", "")
                print(f"\n📊 Ticker Update [{market}]:")
                print(f"   Last: {ticker.get('last')}")
                print(f"   Volume: {ticker.get('volume')}")
                print(f"   High: {ticker.get('high')}")
                print(f"   Low: {ticker.get('low')}")
            
        elif method == "depth.update":
            # Order book update
            depth_data = data.get("data", {})
            market = depth_data.get("market", "")
            asks = depth_data.get("asks", [])
            bids = depth_data.get("bids", [])
            print(f"\n📖 Order Book Update [{market}]:")
            print(f"   Asks: {len(asks)} levels")
            print(f"   Bids: {len(bids)} levels")
            if asks:
                print(f"   Best Ask: {asks[0]}")
            if bids:
                print(f"   Best Bid: {bids[0]}")
                    
        elif method == "deals.update":
            # Trade update
            deals_data = data.get("data", {})
            market = deals_data.get("market", "")
            deals = deals_data.get("deals", [])
            if deals:
                trade = deals[0]
                print(f"\n💰 Trade [{market}]:")
                print(f"   Price: {trade.get('price')}")
                print(f"   Amount: {trade.get('amount')}")
                print(f"   Type: {trade.get('type')}")
                    
        elif method == "order.update":
            # User order update
            print(f"\n📝 Order Update:")
            print(json.dumps(data, indent=2))
            
        elif method == "asset.update":
            # Balance update
            print(f"\n💼 Balance Update:")
            print(json.dumps(data, indent=2))
            
        elif data.get("method") == "server.pong":
            # Pong response
            pass
        else:
            # Other messages
            print(f"\n📩 Received: {json.dumps(data, indent=2)}")
    
    async def close(self):
        """Close the WebSocket connection"""
        if self.websocket:
            await self.websocket.close()
            print("Connection closed")


async def example_public_data():
    """Example: Subscribe to public market data without authentication"""
    print("=" * 60)
    print("Example 1: Public Market Data (No Authentication)")
    print("=" * 60)
    
    client = CoinExWebSocket()
    
    try:
        # Connect to WebSocket
        await client.connect()
        
        # Subscribe to BTC/USDT ticker
        await client.subscribe_ticker("BTCUSDT")
        
        # Subscribe to order book depth
        await client.subscribe_depth("BTCUSDT", limit=5)
        
        # Subscribe to trades
        await client.subscribe_trades("BTCUSDT")
        
        # Listen for messages for 30 seconds
        print("\nListening for updates (30 seconds)...\n")
        await asyncio.wait_for(client.listen(), timeout=30.0)
        
    except asyncio.TimeoutError:
        print("\nTimeout reached")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.close()


async def example_authenticated():
    """Example: Authenticated WebSocket connection for account data"""
    print("\n" + "=" * 60)
    print("Example 2: Authenticated Connection (Account Data)")
    print("=" * 60)
    
    # Replace with your actual credentials
    ACCESS_ID = "your_access_id_here"
    SECRET_KEY = "your_secret_key_here"
    
    if ACCESS_ID == "your_access_id_here":
        print("\n⚠️  Please update ACCESS_ID and SECRET_KEY with your actual credentials")
        return
    
    client = CoinExWebSocket(ACCESS_ID, SECRET_KEY)
    
    try:
        # Connect to WebSocket
        await client.connect()
        
        # Authenticate
        await client.authenticate()
        
        # Subscribe to user-specific data
        await client.subscribe_balance()
        await client.subscribe_user_order()
        await client.subscribe_user_deals()
        
        # Also subscribe to some market data
        await client.subscribe_ticker("BTCUSDT")
        
        # Listen for messages for 60 seconds
        print("\nListening for updates (60 seconds)...\n")
        await asyncio.wait_for(client.listen(), timeout=60.0)
        
    except asyncio.TimeoutError:
        print("\nTimeout reached")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.close()


async def example_with_ping():
    """Example: Connection with periodic ping to keep alive"""
    print("\n" + "=" * 60)
    print("Example 3: Long-running Connection with Ping")
    print("=" * 60)
    
    client = CoinExWebSocket()
    
    try:
        await client.connect()
        await client.subscribe_ticker("BTCUSDT")
        
        async def listen_with_ping():
            """Listen for messages while sending periodic pings"""
            async def ping_task():
                while True:
                    await asyncio.sleep(30)  # Ping every 30 seconds
                    try:
                        await client.ping()
                        print("→ Ping sent")
                    except Exception as e:
                        print(f"Ping failed: {e}")
                        break
            
            # Run ping task and listen task concurrently
            await asyncio.gather(
                ping_task(),
                client.listen(),
                return_exceptions=True
            )
        
        print("\nListening with periodic pings (60 seconds)...\n")
        await asyncio.wait_for(listen_with_ping(), timeout=60.0)
        
    except asyncio.TimeoutError:
        print("\nTimeout reached")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.close()


async def main():
    """Run all examples"""
    # Example 1: Public data (no authentication required)
    await example_public_data()
    
    # Example 2: Authenticated connection (requires API keys)
    # Uncomment to run:
    # await example_authenticated()
    
    # Example 3: Long-running connection with ping
    # Uncomment to run:
    # await example_with_ping()


if __name__ == "__main__":
    asyncio.run(main())
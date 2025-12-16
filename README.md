# CoinEx WebSocket API Authentication Example

This example demonstrates how to authenticate and interact with the CoinEx cryptocurrency exchange WebSocket API for real-time data.

## Important Notes

**CoinEx WebSocket API provides real-time market data and account updates.** You can:

1. Use **public endpoints** without authentication for market data (tickers, order books, trades)
2. Use **authenticated endpoints** with API keys for account data (balance, orders, user trades)

To get API credentials:
1. Create an account on [CoinEx](https://www.coinex.com/)
2. Go to your account settings and generate API keys
3. Use the API Access ID and Secret Key for authentication

## Setup

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. (Optional) Get your API credentials for authenticated features:
   - Log into your CoinEx account
   - Navigate to: Account Settings → API Management
   - Create a new API key pair
   - Copy your Access ID and Secret Key

3. Update the script with your credentials (for authenticated examples):
   - Open `coinex_websocket.py`
   - Replace `your_access_id_here` with your actual Access ID
   - Replace `your_secret_key_here` with your actual Secret Key

## Security Best Practices

**NEVER commit your API keys to version control!**

Instead, use environment variables:

```python
import os

ACCESS_ID = os.environ.get('COINEX_ACCESS_ID')
SECRET_KEY = os.environ.get('COINEX_SECRET_KEY')
```

Then set them in your environment:
```bash
export COINEX_ACCESS_ID="your_access_id"
export COINEX_SECRET_KEY="your_secret_key"
```

## Usage

Run the example script:
```bash
python coinex_websocket.py
```

## What the Script Does

The script includes three examples:

### Example 1: Public Market Data (No Authentication)
- Connects to CoinEx WebSocket
- Subscribes to BTC/USDT ticker updates
- Subscribes to order book depth
- Subscribes to real-time trades
- Runs for 30 seconds

### Example 2: Authenticated Connection (Requires API Keys)
- Authenticates with your API credentials
- Subscribes to balance updates
- Subscribes to your order updates
- Subscribes to your trade history
- Also monitors market data
- Runs for 60 seconds

### Example 3: Long-running Connection with Ping
- Demonstrates how to keep the connection alive
- Sends periodic ping messages every 30 seconds
- Prevents timeout on long-running connections

## Available Subscriptions

### Public Subscriptions (No Auth Required)
- `subscribe_ticker(market)` - Real-time ticker data
- `subscribe_depth(market, limit, interval)` - Order book depth
- `subscribe_trades(market)` - Recent trades

### Authenticated Subscriptions (Auth Required)
- `subscribe_balance()` - Your balance updates
- `subscribe_user_order()` - Your order updates
- `subscribe_user_deals()` - Your trade history

## WebSocket Authentication Method

CoinEx WebSocket uses HMAC-SHA256 signature authentication:

1. Generate a timestamp in milliseconds
2. Create HMAC-SHA256 signature of the timestamp using your secret key
3. Send authentication message with access_id, timestamp, and signature
4. Wait for successful authentication response

## Connection Management

- The WebSocket connection may timeout after inactivity
- Send periodic `ping()` messages to keep the connection alive
- The script includes an example of ping management

## API Documentation

For complete WebSocket API documentation, visit:
- https://docs.coinex.com/api/v2/spot/ws/

## Troubleshooting

- **"Invalid signature" error**: Check that your secret key is correct and the timestamp generation is accurate
- **"Access denied" error**: Verify your API key has the necessary permissions enabled
- **Connection timeout**: Implement periodic ping messages to keep connection alive
- **"Authentication required" error**: Make sure you've called `authenticate()` before subscribing to private channels

## Message Format

Messages are in JSON format. Example message types:

```json
// Ticker update
{
  "method": "state.update",
  "params": [{
    "market": "BTCUSDT",
    "last": "50000.00",
    "volume": "1234.56"
  }]
}

// Order book update
{
  "method": "depth.update",
  "params": [true, "BTCUSDT", {"asks": [...], "bids": [...]}]
}

// Trade update
{
  "method": "deals.update",
  "params": ["BTCUSDT", [{"price": "50000", "amount": "0.1", "type": "buy"}]]
}
```

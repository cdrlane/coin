"""
Debug script to see exactly what CoinEx WebSocket is sending
"""

import asyncio
import json
import websockets
import gzip
import time


async def debug_coinex():
    """Connect and print all messages we receive"""
    
    ws_url = "wss://socket.coinex.com/v2/spot"
    
    print("=" * 80)
    print("COINEX WEBSOCKET DEBUG")
    print("=" * 80)
    print(f"\nConnecting to: {ws_url}")
    
    try:
        # Try with deflate compression
        websocket = await websockets.connect(ws_url, compression='deflate')
        print("✓ Connected with deflate compression\n")
        
        # Subscribe to ticker
        subscribe_message = {
            "method": "state.subscribe",
            "params": {
                "market_list": ["BTCUSDT"]
            },
            "id": 1
        }
        
        print("Sending subscription:")
        print(json.dumps(subscribe_message, indent=2))
        await websocket.send(json.dumps(subscribe_message))
        print("\nWaiting for messages...\n")
        print("=" * 80)
        
        message_count = 0
        
        # Listen for 60 seconds
        while message_count < 10:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=60.0)
                message_count += 1
                
                print(f"\n📨 MESSAGE #{message_count}")
                print("-" * 80)
                
                # Check message type
                if isinstance(message, bytes):
                    print(f"Type: BYTES (length: {len(message)})")
                    print(f"First 20 bytes: {message[:20].hex()}")
                    
                    # Check if gzip
                    if len(message) > 1 and message[0] == 0x1f and message[1] == 0x8b:
                        print("Format: GZIP compressed")
                        try:
                            decompressed = gzip.decompress(message)
                            message_str = decompressed.decode('utf-8')
                            print(f"✓ Decompressed successfully ({len(decompressed)} bytes)")
                        except Exception as e:
                            print(f"✗ Decompression failed: {e}")
                            continue
                    else:
                        print("Format: Raw bytes (not gzip)")
                        try:
                            message_str = message.decode('utf-8')
                            print("✓ Decoded as UTF-8")
                        except Exception as e:
                            print(f"✗ Decode failed: {e}")
                            continue
                else:
                    print(f"Type: STRING (length: {len(message)})")
                    message_str = message
                
                # Try to parse JSON
                try:
                    data = json.loads(message_str)
                    print("\n✓ Valid JSON")
                    print("\nParsed Message:")
                    print(json.dumps(data, indent=2))
                    
                    # Analyze the message
                    print("\nMessage Analysis:")
                    method = data.get("method", "N/A")
                    print(f"  Method: {method}")
                    
                    if "id" in data:
                        print(f"  ID: {data['id']}")
                    
                    if "result" in data:
                        print(f"  Result: {data['result']}")
                    
                    if "error" in data:
                        print(f"  Error: {data['error']}")
                    
                    if "params" in data:
                        params = data.get("params", [])
                        print(f"  Params type: {type(params)}")
                        print(f"  Params length: {len(params) if isinstance(params, list) else 'N/A'}")
                        if isinstance(params, list) and len(params) > 0:
                            print(f"  First param: {params[0]}")
                    
                except json.JSONDecodeError as e:
                    print(f"\n✗ JSON parse error: {e}")
                    print(f"Raw string (first 500 chars): {message_str[:500]}")
                
                print("-" * 80)
                
            except asyncio.TimeoutError:
                print("\n⏱️  Timeout - no messages in 60 seconds")
                break
            except Exception as e:
                print(f"\n✗ Error receiving message: {e}")
                import traceback
                traceback.print_exc()
                break
        
        print("\n" + "=" * 80)
        print(f"Total messages received: {message_count}")
        print("=" * 80)
        
        await websocket.close()
        
    except Exception as e:
        print(f"\n✗ Connection error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(debug_coinex())
"""
CoinEx Market Lister

Simple script to get all available markets from CoinEx.
"""

import requests
from typing import List


def get_all_markets() -> List[dict]:
    """Get all available markets with details"""
    try:
        url = "https://api.coinex.com/v2/spot/market"
        
        print("Fetching markets from CoinEx...")
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get("code") == 0:
                markets = data.get("data", [])
                print(f"✓ Found {len(markets)} markets\n")
                return markets
            else:
                print(f"❌ API Error: {data.get('message')}")
                return []
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Error: {e}")
        return []


def filter_by_quote(markets: List[dict], quote_currency: str) -> List[dict]:
    """Filter markets by quote currency"""
    return [m for m in markets if m.get('market', '').endswith(quote_currency)]


def display_markets(markets: List[dict], show_details: bool = False):
    """Display markets in a nice format"""
    if not markets:
        print("No markets found")
        return
    
    print(f"{'='*100}")
    print(f"MARKETS ({len(markets)} total)")
    print(f"{'='*100}\n")
    
    if show_details:
        # Show detailed info
        print(f"{'Market':<15} {'Min Amount':<15} {'Maker Fee':<12} {'Taker Fee':<12} {'Status':<10}")
        print("-"*100)
        
        for m in markets:
            market = m.get('market', 'N/A')
            min_amount = m.get('min_amount', 'N/A')
            maker_fee = m.get('maker_fee_rate', 'N/A')
            taker_fee = m.get('taker_fee_rate', 'N/A')
            is_trading = "Active" if m.get('is_market_allowed', False) else "Inactive"
            
            print(f"{market:<15} {min_amount:<15} {maker_fee:<12} {taker_fee:<12} {is_trading:<10}")
    else:
        # Just show market names in columns
        market_names = [m.get('market', '') for m in markets]
        
        cols = 5
        for i in range(0, len(market_names), cols):
            row = market_names[i:i+cols]
            print("  ".join(f"{m:<15}" for m in row))
    
    print(f"\n{'='*100}\n")


def save_to_file(markets: List[dict], filename: str = "coinex_markets.txt"):
    """Save market list to file"""
    try:
        with open(filename, 'w') as f:
            f.write("# CoinEx Markets List\n")
            f.write(f"# Total: {len(markets)} markets\n\n")
            
            for m in markets:
                market = m.get('market', '')
                f.write(f"{market}\n")
        
        print(f"✓ Saved to {filename}")
    except Exception as e:
        print(f"❌ Error saving: {e}")


def main():
    print("\n" + "="*100)
    print(" "*35 + "📊 COINEX MARKET LISTER 📊")
    print("="*100 + "\n")
    
    print("Options:")
    print("1. List all markets")
    print("2. List USDT markets only")
    print("3. List BTC markets only")
    print("4. Filter by custom quote currency")
    print("5. Show detailed market info")
    print()
    
    choice = input("Select option (1-5): ").strip()
    
    # Fetch all markets
    all_markets = get_all_markets()
    
    if not all_markets:
        return
    
    if choice == "1":
        # All markets
        display_markets(all_markets)
        
    elif choice == "2":
        # USDT markets
        usdt_markets = filter_by_quote(all_markets, "USDT")
        print(f"\n🔍 Found {len(usdt_markets)} USDT markets\n")
        display_markets(usdt_markets)
        
    elif choice == "3":
        # BTC markets
        btc_markets = filter_by_quote(all_markets, "BTC")
        print(f"\n🔍 Found {len(btc_markets)} BTC markets\n")
        display_markets(btc_markets)
        
    elif choice == "4":
        # Custom filter
        quote = input("Enter quote currency (e.g., ETH, USDC): ").strip().upper()
        filtered = filter_by_quote(all_markets, quote)
        print(f"\n🔍 Found {len(filtered)} {quote} markets\n")
        display_markets(filtered)
        
    elif choice == "5":
        # Detailed info
        print("\nSelect filter:")
        print("1. All markets (detailed)")
        print("2. USDT markets (detailed)")
        sub_choice = input("Choice: ").strip()
        
        if sub_choice == "2":
            markets_to_show = filter_by_quote(all_markets, "USDT")
        else:
            markets_to_show = all_markets
        
        display_markets(markets_to_show, show_details=True)
    
    else:
        print("❌ Invalid choice")
        return
    
    # Offer to save
    save = input("\nSave list to file? (y/n): ").strip().lower()
    if save == 'y':
        filename = input("Filename (default: coinex_markets.txt): ").strip()
        if not filename:
            filename = "coinex_markets.txt"
        
        # Extract just market names for saving
        markets_to_save = all_markets if choice == "1" else filter_by_quote(all_markets, "USDT")
        save_to_file(markets_to_save, filename)


if __name__ == "__main__":
    main()
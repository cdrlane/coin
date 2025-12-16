"""
CoinEx Daily Close Data Fetcher

Fetches historical daily OHLCV (Open, High, Low, Close, Volume) data from CoinEx REST API.
Saves to CSV file.
"""

import requests
import csv
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional

try:
    import pymannkendall as mk
    HAS_MK = True
except ImportError:
    HAS_MK = False
    print("⚠️  pymannkendall not installed. Trend analysis will be disabled.")
    print("   Install with: pip install pymannkendall")



class CoinExDailyData:
    """Fetch daily candlestick data from CoinEx"""
    
    def __init__(self):
        self.base_url = "https://api.coinex.com/v2"
    
    def get_all_markets(self) -> List[str]:
        """
        Get list of all available markets on CoinEx
        
        Returns:
            List of market symbols
        """
        try:
            url = f"{self.base_url}/spot/market"
            
            print("Fetching all available markets from CoinEx...")
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get("code") == 0:
                    markets_data = data.get("data", [])
                    
                    # Extract market names
                    markets = [m.get("market") for m in markets_data if m.get("market")]
                    
                    print(f"✓ Found {len(markets)} markets")
                    return sorted(markets)
                else:
                    print(f"❌ API Error: {data.get('message')}")
                    return []
            else:
                print(f"❌ HTTP Error: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"❌ Error fetching markets: {e}")
            return []
    
    def filter_markets(self, markets: List[str], quote_currency: str = "USDT") -> List[str]:
        """
        Filter markets by quote currency
        
        Args:
            markets: List of all markets
            quote_currency: Quote currency to filter (e.g., 'USDT', 'BTC', 'ETH')
            
        Returns:
            Filtered list of markets
        """
        return [m for m in markets if m.endswith(quote_currency)]
    
    def display_markets(self, markets: List[str], per_page: int = 50):
        """
        Display markets in a formatted way
        
        Args:
            markets: List of markets to display
            per_page: Number of markets to show per page
        """
        print(f"\n{'='*80}")
        print(f"AVAILABLE MARKETS ({len(markets)} total)")
        print(f"{'='*80}\n")
        
        for i in range(0, len(markets), per_page):
            page_markets = markets[i:i+per_page]
            
            # Display in columns
            cols = 5
            for j in range(0, len(page_markets), cols):
                row = page_markets[j:j+cols]
                print("  ".join(f"{m:<15}" for m in row))
            
            # Pagination
            if i + per_page < len(markets):
                print(f"\n--- Showing {i+1}-{min(i+per_page, len(markets))} of {len(markets)} ---")
                cont = input("Press Enter to see more (or 'q' to quit): ").strip().lower()
                if cont == 'q':
                    break
                print()
        
        print(f"\n{'='*80}\n")
    
    def analyze_trend(self, data: List[Dict]) -> Dict:
        """
        Analyze trend using Mann-Kendall test
        
        Args:
            data: Parsed kline data with Close prices
            
        Returns:
            Dictionary with trend analysis results
        """
        if not HAS_MK:
            return {
                'error': 'pymannkendall not installed',
                'trend': 'unknown',
                'p_value': None,
                'tau': None,
                'slope': None,
                'z_score': None
            }
        
        if not data or len(data) < 3:
            return {
                'error': 'insufficient data',
                'trend': 'unknown',
                'p_value': None,
                'tau': None,
                'slope': None,
                'z_score': None
            }
        
        try:
            # Extract close prices
            close_prices = [float(d['Close']) for d in data]
            
            # Run Mann-Kendall test
            result = mk.original_test(close_prices)
            
            return {
                'trend': result.trend,           # 'increasing', 'decreasing', 'no trend'
                'p_value': result.p,             # Statistical significance
                'tau': result.tau,               # Kendall's tau (-1 to 1)
                'slope': result.slope,           # Sen's slope (rate of change)
                'z_score': result.z,             # Z-score
                'h': result.h,                   # True if trend is significant
                'significance': 'significant' if result.h else 'not significant'
            }
        except Exception as e:
            return {
                'error': str(e),
                'trend': 'error',
                'p_value': None,
                'tau': None,
                'slope': None,
                'z_score': None
            }
    
    def analyze_all_usdt_trends(self, days: int = 365, min_data_points: int = 30):
        """
        Analyze trends for all USDT markets and rank by trend strength
        
        Args:
            days: Number of days to analyze
            min_data_points: Minimum data points required for analysis
            
        Returns:
            List of markets sorted by trend strength
        """
        print(f"\n{'='*100}")
        print(f"MANN-KENDALL TREND ANALYSIS - ALL USDT MARKETS")
        print(f"{'='*100}\n")
        
        if not HAS_MK:
            print("❌ pymannkendall not installed")
            print("   Install with: pip install pymannkendall")
            return []
        
        # Get all markets
        all_markets = self.get_all_markets()
        if not all_markets:
            return []
        
        # Filter for USDT markets
        usdt_markets = [m for m in all_markets if m.endswith('USDT')]
        
        print(f"Found {len(usdt_markets)} USDT markets")
        print(f"Analyzing {days} days of data\n")
        
        proceed = input(f"Analyze {len(usdt_markets)} markets? (y/n): ").strip().lower()
        if proceed != 'y':
            return []
        
        results = []
        
        for i, market in enumerate(usdt_markets, 1):
            if i % 50 == 0:
                print(f"Progress: {i}/{len(usdt_markets)} markets analyzed...")
            
            try:
                # Fetch data
                klines = self.get_daily_klines(market, limit=days)
                if not klines:
                    continue
                
                # Parse data
                parsed_data = self.parse_kline_data(klines)
                if not parsed_data or len(parsed_data) < min_data_points:
                    continue
                
                # Analyze trend
                trend_result = self.analyze_trend(parsed_data)
                
                if trend_result.get('trend') != 'error':
                    # Calculate additional metrics
                    first_close = parsed_data[0]['Close']
                    last_close = parsed_data[-1]['Close']
                    total_change_pct = ((last_close - first_close) / first_close * 100)
                    
                    results.append({
                        'market': market,
                        'trend': trend_result['trend'],
                        'p_value': trend_result['p_value'],
                        'tau': trend_result['tau'],
                        'slope': trend_result['slope'],
                        'z_score': trend_result['z_score'],
                        'significance': trend_result['significance'],
                        'data_points': len(parsed_data),
                        'first_close': first_close,
                        'last_close': last_close,
                        'total_change_%': total_change_pct
                    })
                
                # Rate limiting
                time.sleep(0.3)
                
            except Exception as e:
                continue
        
        print(f"\n✓ Analysis complete: {len(results)} markets analyzed\n")
        
        return results
    
    def display_trend_results(self, results: List[Dict], top_n: int = 20):
        """
        Display trend analysis results
        
        Args:
            results: List of trend analysis results
            top_n: Number of top results to display
        """
        if not results:
            print("No results to display")
            return
        
        # Separate by trend type
        increasing = [r for r in results if r['trend'] == 'increasing']
        decreasing = [r for r in results if r['trend'] == 'decreasing']
        no_trend = [r for r in results if r['trend'] == 'no trend']
        
        print(f"\n{'='*100}")
        print(f"TREND ANALYSIS SUMMARY")
        print(f"{'='*100}")
        print(f"Total markets analyzed:  {len(results)}")
        print(f"Increasing trends:       {len(increasing)} ({len(increasing)/len(results)*100:.1f}%)")
        print(f"Decreasing trends:       {len(decreasing)} ({len(decreasing)/len(results)*100:.1f}%)")
        print(f"No significant trend:    {len(no_trend)} ({len(no_trend)/len(results)*100:.1f}%)")
        print(f"{'='*100}\n")
        
        # Display top increasing trends (strongest uptrends)
        if increasing:
            print(f"\n{'='*100}")
            print(f"TOP {min(top_n, len(increasing))} STRONGEST UPTRENDS (by Kendall's Tau)")
            print(f"{'='*100}")
            print(f"{'Market':<15} {'Tau':<8} {'P-Value':<10} {'Z-Score':<10} {'Slope':<12} {'Change%':<10} {'Signif.':<12}")
            print("-"*100)
            
            # Sort by tau (strongest positive correlation)
            increasing_sorted = sorted(increasing, key=lambda x: x['tau'], reverse=True)[:top_n]
            
            for r in increasing_sorted:
                print(f"{r['market']:<15} {r['tau']:>7.4f} {r['p_value']:>9.6f} {r['z_score']:>9.2f} "
                      f"{r['slope']:>11.4f} {r['total_change_%']:>9.2f}% {r['significance']:<12}")
        
        # Display top decreasing trends (strongest downtrends)
        if decreasing:
            print(f"\n{'='*100}")
            print(f"TOP {min(top_n, len(decreasing))} STRONGEST DOWNTRENDS (by Kendall's Tau)")
            print(f"{'='*100}")
            print(f"{'Market':<15} {'Tau':<8} {'P-Value':<10} {'Z-Score':<10} {'Slope':<12} {'Change%':<10} {'Signif.':<12}")
            print("-"*100)
            
            # Sort by tau (strongest negative correlation)
            decreasing_sorted = sorted(decreasing, key=lambda x: x['tau'])[:top_n]
            
            for r in decreasing_sorted:
                print(f"{r['market']:<15} {r['tau']:>7.4f} {r['p_value']:>9.6f} {r['z_score']:>9.2f} "
                      f"{r['slope']:>11.4f} {r['total_change_%']:>9.2f}% {r['significance']:<12}")
        
        print(f"\n{'='*100}\n")
    
    def save_trend_results(self, results: List[Dict], filename: str = "trend_analysis_results.csv"):
        """Save trend analysis results to CSV"""
        if not results:
            print("No results to save")
            return
        
        try:
            with open(filename, 'w', newline='') as f:
                fieldnames = ['market', 'trend', 'p_value', 'tau', 'slope', 'z_score', 
                            'significance', 'data_points', 'first_close', 'last_close', 'total_change_%']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
            
            print(f"✓ Saved {len(results)} trend analysis results to {filename}")
        except Exception as e:
            print(f"❌ Error saving results: {e}")
    
    def save_markets_list(self, markets: List[str], filename: str = "coinex_markets.txt"):
        """Save trend analysis results to CSV"""
        if not results:
            print("No results to save")
            return
        
        try:
            with open(filename, 'w', newline='') as f:
                fieldnames = ['market', 'trend', 'p_value', 'tau', 'slope', 'z_score', 
                            'significance', 'data_points', 'first_close', 'last_close', 'total_change_%']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
            
            print(f"✓ Saved {len(results)} trend analysis results to {filename}")
        except Exception as e:
            print(f"❌ Error saving results: {e}")
        """
        Save list of markets to a text file
        
        Args:
            markets: List of markets
            filename: Output filename
        """
        try:
            with open(filename, 'w') as f:
                for market in markets:
                    f.write(f"{market}\n")
            
            print(f"✓ Saved {len(markets)} markets to {filename}")
        except Exception as e:
            print(f"❌ Error saving markets list: {e}")
        
    def get_daily_klines(self, market: str, limit: int = 1000) -> List[Dict]:
        """
        Get daily candlestick data for a market
        
        Args:
            market: Market symbol (e.g., 'BTCUSDT')
            limit: Number of days to fetch (max 1000)
            
        Returns:
            List of daily candles with OHLCV data
        """
        try:
            url = f"{self.base_url}/spot/kline"
            params = {
                "market": market,
                "period": "1day",  # Daily candles
                "limit": limit
            }
            
            print(f"Fetching {limit} days of data for {market}...")
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get("code") == 0:
                    klines = data.get("data", [])
                    print(f"✓ Retrieved {len(klines)} daily candles")
                    
                    # Debug: Show structure of first kline
                    if klines and len(klines) > 0:
                        print(f"\n📊 Sample kline structure:")
                        print(f"   Type: {type(klines[0])}")
                        print(f"   Length: {len(klines[0]) if isinstance(klines[0], (list, tuple)) else 'N/A'}")
                        print(f"   First kline: {klines[0]}")
                        print()
                    
                    return klines
                else:
                    print(f"❌ API Error: {data.get('message')}")
                    return []
            else:
                print(f"❌ HTTP Error: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"❌ Error fetching data: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def parse_kline_data(self, klines: List) -> List[Dict]:
        """
        Parse kline data into readable format
        
        CoinEx v2 may return dictionaries or arrays
        """
        parsed_data = []
        
        for i, kline in enumerate(klines):
            try:
                # Check if kline is a dictionary or list
                if isinstance(kline, dict):
                    # Dictionary format
                    # Timestamp is in milliseconds, convert to seconds
                    timestamp_ms = int(kline.get('created_at', kline.get('timestamp', 0)))
                    timestamp = timestamp_ms // 1000  # Convert milliseconds to seconds
                    dt = datetime.fromtimestamp(timestamp)
                    
                    parsed_data.append({
                        'Date': dt.strftime('%Y-%m-%d'),
                        'Timestamp': dt.strftime('%Y-%m-%d %H:%M:%S'),
                        'Unix_Timestamp': timestamp,
                        'Open': float(kline.get('open', 0)),
                        'Close': float(kline.get('close', 0)),
                        'High': float(kline.get('high', 0)),
                        'Low': float(kline.get('low', 0)),
                        'Volume': float(kline.get('volume', 0)),
                        'Value': float(kline.get('value', kline.get('amount', 0))),
                        'Market': kline.get('market', '')
                    })
                    
                elif isinstance(kline, (list, tuple)):
                    # Array format: [timestamp, open, close, high, low, volume, value, market]
                    if len(kline) < 7:
                        print(f"⚠️  Skipping invalid kline at index {i}: too short")
                        continue
                    
                    # Check if timestamp is in milliseconds (> year 2100 in seconds)
                    timestamp_val = int(kline[0])
                    timestamp = timestamp_val // 1000 if timestamp_val > 4000000000 else timestamp_val
                    dt = datetime.fromtimestamp(timestamp)
                    
                    parsed_data.append({
                        'Date': dt.strftime('%Y-%m-%d'),
                        'Timestamp': dt.strftime('%Y-%m-%d %H:%M:%S'),
                        'Unix_Timestamp': timestamp,
                        'Open': float(kline[1]),
                        'Close': float(kline[2]),
                        'High': float(kline[3]),
                        'Low': float(kline[4]),
                        'Volume': float(kline[5]),
                        'Value': float(kline[6]),
                        'Market': kline[7] if len(kline) > 7 else ''
                    })
                else:
                    print(f"⚠️  Unknown kline format at index {i}: {type(kline)}")
                    continue
                
            except (IndexError, ValueError, TypeError, KeyError) as e:
                print(f"⚠️  Error parsing kline at index {i}: {e}")
                print(f"    Kline data: {kline}")
                continue
        
        return parsed_data
    
    def save_to_csv(self, data: List[Dict], filename: str, append: bool = False):
        """
        Save parsed data to CSV
        
        Args:
            data: Parsed kline data
            filename: Output filename
            append: If True, append to existing file; if False, overwrite
        """
        if not data:
            print("❌ No data to save")
            return
        
        import os
        file_exists = os.path.exists(filename)
        
        try:
            mode = 'a' if append else 'w'
            with open(filename, mode, newline='') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                
                # Only write header if file doesn't exist or we're overwriting
                if not file_exists or not append:
                    writer.writeheader()
                
                writer.writerows(data)
            
            if append and file_exists:
                print(f"✓ Appended {len(data)} records to {filename}")
            else:
                print(f"✓ Saved {len(data)} records to {filename}")
            
        except Exception as e:
            print(f"❌ Error saving CSV: {e}")
    
    def fetch_and_save(self, market: str, days: int = 365, filename: Optional[str] = None, append: bool = False):
        """
        Fetch daily data and save to CSV
        
        Args:
            market: Market symbol (e.g., 'BTCUSDT')
            days: Number of days to fetch (max 1000)
            filename: Output CSV filename (auto-generated if None)
            append: If True, append to existing file instead of overwriting
        """
        # Fetch data
        klines = self.get_daily_klines(market, limit=days)
        
        if not klines:
            return
        
        # Parse data
        parsed_data = self.parse_kline_data(klines)
        
        # Generate filename if not provided
        if filename is None:
            filename = f"{market}_daily_{days}days_{datetime.now().strftime('%Y%m%d')}.csv"
        
        # Save to CSV (append or overwrite based on parameter)
        self.save_to_csv(parsed_data, filename, append=append)
        
        # Print summary
        if parsed_data:
            print(f"\n📊 Data Summary:")
            print(f"   Market:      {market}")
            print(f"   Records:     {len(parsed_data)}")
            print(f"   Date Range:  {parsed_data[0]['Date']} to {parsed_data[-1]['Date']}")
            print(f"   First Close: ${parsed_data[0]['Close']:,.2f}")
            print(f"   Last Close:  ${parsed_data[-1]['Close']:,.2f}")
            
            # Calculate change
            first_close = parsed_data[0]['Close']
            last_close = parsed_data[-1]['Close']
            change_pct = ((last_close - first_close) / first_close) * 100
            print(f"   Change:      {change_pct:+.2f}%")
    
    def fetch_all_usdt_markets(self, days: int = 365, filename: str = "all_usdt_markets_daily.csv"):
        """
        Fetch data for ALL USDT markets on CoinEx
        
        Args:
            days: Number of days to fetch
            filename: Output CSV filename
        """
        print(f"\n{'='*80}")
        print(f"Fetching ALL USDT Markets")
        print(f"{'='*80}\n")
        
        # Get all markets
        all_markets = self.get_all_markets()
        
        if not all_markets:
            print("❌ Could not retrieve markets list")
            return
        
        # Filter for USDT markets only
        usdt_markets = [m for m in all_markets if m.endswith('USDT')]
        
        print(f"Found {len(usdt_markets)} USDT markets")
        print(f"Fetching {days} days of data for each market")
        print(f"Output file: {filename}\n")
        
        proceed = input(f"This will fetch {len(usdt_markets)} markets. Continue? (y/n): ").strip().lower()
        if proceed != 'y':
            print("❌ Cancelled")
            return
        
        import os
        # Remove existing file if it exists
        if os.path.exists(filename):
            os.remove(filename)
            print(f"🗑️  Removed existing file: {filename}\n")
        
        total_records = 0
        successful = 0
        failed = 0
        
        for i, market in enumerate(usdt_markets, 1):
            print(f"\n[{i}/{len(usdt_markets)}] Processing {market}...")
            
            try:
                klines = self.get_daily_klines(market, limit=days)
                
                if klines:
                    parsed_data = self.parse_kline_data(klines)
                    
                    if parsed_data:
                        # Append to single file
                        self.save_to_csv(parsed_data, filename, append=(i > 1))
                        total_records += len(parsed_data)
                        successful += 1
                        
                        # Brief summary
                        print(f"   ✓ Saved {len(parsed_data)} records")
                    else:
                        print(f"   ⚠️  No valid data")
                        failed += 1
                else:
                    print(f"   ⚠️  No data returned")
                    failed += 1
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
                failed += 1
            
            # Rate limiting - be nice to the API
            if i < len(usdt_markets):
                time.sleep(0.5)  # Small delay between requests
            
            # Progress update every 50 markets
            if i % 50 == 0:
                print(f"\n--- Progress: {i}/{len(usdt_markets)} markets processed ---")
                print(f"    Successful: {successful} | Failed: {failed} | Total records: {total_records}\n")
        
        print(f"\n{'='*80}")
        print(f"✓ Completed! All USDT markets data saved to: {filename}")
        print(f"   Total markets attempted: {len(usdt_markets)}")
        print(f"   Successful:              {successful}")
        print(f"   Failed:                  {failed}")
        print(f"   Total records:           {total_records}")
        print(f"{'='*80}")
    
    def fetch_multiple_markets(self, markets: List[str], days: int = 365, filename: str = "all_markets_daily.csv"):
        """
        Fetch data for ALL USDT markets on CoinEx
        
        Args:
            days: Number of days to fetch
            filename: Output CSV filename
        """
        print(f"\n{'='*80}")
        print(f"Fetching ALL USDT Markets")
        print(f"{'='*80}\n")
        
        # Get all markets
        all_markets = self.get_all_markets()
        
        if not all_markets:
            print("❌ Could not retrieve markets list")
            return
        
        # Filter for USDT markets only
        usdt_markets = [m for m in all_markets if m.endswith('USDT')]
        
        print(f"Found {len(usdt_markets)} USDT markets")
        print(f"Fetching {days} days of data for each market")
        print(f"Output file: {filename}\n")
        
        proceed = input(f"This will fetch {len(usdt_markets)} markets. Continue? (y/n): ").strip().lower()
        if proceed != 'y':
            print("❌ Cancelled")
            return
        
        import os
        # Remove existing file if it exists
        if os.path.exists(filename):
            os.remove(filename)
            print(f"🗑️  Removed existing file: {filename}\n")
        
        total_records = 0
        successful = 0
        failed = 0
        
        for i, market in enumerate(usdt_markets, 1):
            print(f"\n[{i}/{len(usdt_markets)}] Processing {market}...")
            
            try:
                klines = self.get_daily_klines(market, limit=days)
                
                if klines:
                    parsed_data = self.parse_kline_data(klines)
                    
                    if parsed_data:
                        # Append to single file
                        self.save_to_csv(parsed_data, filename, append=(i > 1))
                        total_records += len(parsed_data)
                        successful += 1
                        
                        # Brief summary
                        print(f"   ✓ Saved {len(parsed_data)} records")
                    else:
                        print(f"   ⚠️  No valid data")
                        failed += 1
                else:
                    print(f"   ⚠️  No data returned")
                    failed += 1
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
                failed += 1
            
            # Rate limiting - be nice to the API
            if i < len(usdt_markets):
                time.sleep(0.5)  # Small delay between requests
            
            # Progress update every 50 markets
            if i % 50 == 0:
                print(f"\n--- Progress: {i}/{len(usdt_markets)} markets processed ---")
                print(f"    Successful: {successful} | Failed: {failed} | Total records: {total_records}\n")
        
        print(f"\n{'='*80}")
        print(f"✓ Completed! All USDT markets data saved to: {filename}")
        print(f"   Total markets attempted: {len(usdt_markets)}")
        print(f"   Successful:              {successful}")
        print(f"   Failed:                  {failed}")
        print(f"   Total records:           {total_records}")
        print(f"{'='*80}")
        """
        Fetch data for multiple markets and save to ONE file
        
        Args:
            markets: List of market symbols
            days: Number of days to fetch
            filename: Single output file for all markets
        """
        print(f"\n{'='*80}")
        print(f"Fetching {days} days of data for {len(markets)} markets")
        print(f"Output file: {filename}")
        print(f"{'='*80}\n")
        
        import os
        # Remove existing file if it exists (start fresh)
        if os.path.exists(filename):
            os.remove(filename)
            print(f"🗑️  Removed existing file: {filename}\n")
        
        total_records = 0
        
        for i, market in enumerate(markets, 1):
            print(f"\n[{i}/{len(markets)}] Processing {market}...")
            print("-" * 80)
            
            # Fetch and append to the same file
            klines = self.get_daily_klines(market, limit=days)
            
            if klines:
                parsed_data = self.parse_kline_data(klines)
                
                if parsed_data:
                    # Append to single file (first market creates file, rest append)
                    self.save_to_csv(parsed_data, filename, append=(i > 1))
                    total_records += len(parsed_data)
                    
                    # Print brief summary
                    print(f"   First Close: ${parsed_data[0]['Close']:,.2f}")
                    print(f"   Last Close:  ${parsed_data[-1]['Close']:,.2f}")
                    first_close = parsed_data[0]['Close']
                    last_close = parsed_data[-1]['Close']
                    change_pct = ((last_close - first_close) / first_close) * 100
                    print(f"   Change:      {change_pct:+.2f}%")
            
            # Rate limiting - be nice to the API
            if i < len(markets):
                time.sleep(1)
        
        print(f"\n{'='*80}")
        print(f"✓ Completed! All data saved to: {filename}")
        print(f"   Total markets:  {len(markets)}")
        print(f"   Total records:  {total_records}")
        print(f"{'='*80}")


def main():
    """Main function with examples"""
    
    print("\n" + "="*80)
    print(" " * 25 + "📊 COINEX DAILY DATA FETCHER 📊")
    print("="*80 + "\n")
    
    fetcher = CoinExDailyData()
    
    # ============= CONFIGURATION =============
    
    # Option 1: Fetch single market
    SINGLE_MARKET = "BTCUSDT"
    DAYS = 365  # Get last 365 days (1 year)
    
    # Option 2: Fetch multiple markets (ALL IN ONE FILE)
    MARKETS = [
        "BTCUSDT",
        "ETHUSDT",
        "BNBUSDT",
        "SOLUSDT",
        "XRPUSDT",
        "ADAUSDT",
        "DOGEUSDT",
        "TRXUSDT",
    ]
    
    ALL_MARKETS_FILE = "all_markets_daily.csv"  # Single file for all markets
    
    # =========================================
    
    print("Select option:")
    print("1. Fetch single market")
    print("2. Fetch multiple markets (save to ONE file)")
    print("3. List all available markets")
    print("4. List markets by quote currency (e.g., USDT, BTC)")
    print("5. Fetch ALL USDT markets (automatic)")
    print("6. Analyze trends for ALL USDT markets (Mann-Kendall)")
    print()
    
    choice = input("Enter choice (1-6): ").strip()
    
    if choice == "1":
        market = input(f"Enter market (default: {SINGLE_MARKET}): ").strip().upper()
        if not market:
            market = SINGLE_MARKET
        
        days_input = input(f"Enter number of days (default: {DAYS}, max: 1000): ").strip()
        days = int(days_input) if days_input else DAYS
        
        fetcher.fetch_and_save(market, days)
        
    elif choice == "2":
        days_input = input(f"Enter number of days (default: {DAYS}, max: 1000): ").strip()
        days = int(days_input) if days_input else DAYS
        
        filename_input = input(f"Output filename (default: {ALL_MARKETS_FILE}): ").strip()
        filename = filename_input if filename_input else ALL_MARKETS_FILE
        
        fetcher.fetch_multiple_markets(MARKETS, days, filename)
        
    elif choice == "3":
        # List all markets
        all_markets = fetcher.get_all_markets()
        
        if all_markets:
            fetcher.display_markets(all_markets)
            
            # Offer to save
            save = input("Save list to file? (y/n): ").strip().lower()
            if save == 'y':
                filename = input("Filename (default: coinex_markets.txt): ").strip()
                if not filename:
                    filename = "coinex_markets.txt"
                fetcher.save_markets_list(all_markets, filename)
    
    elif choice == "4":
        # List markets by quote currency
        quote = input("Enter quote currency (e.g., USDT, BTC, ETH): ").strip().upper()
        
        all_markets = fetcher.get_all_markets()
        
        if all_markets:
            filtered = fetcher.filter_markets(all_markets, quote)
            
            if filtered:
                fetcher.display_markets(filtered)
                
                # Offer to save
                save = input("Save filtered list to file? (y/n): ").strip().lower()
                if save == 'y':
                    filename = input(f"Filename (default: coinex_{quote}_markets.txt): ").strip()
                    if not filename:
                        filename = f"coinex_{quote}_markets.txt"
                    fetcher.save_markets_list(filtered, filename)
            else:
                print(f"❌ No markets found with quote currency: {quote}")
    
    elif choice == "5":
        # Fetch ALL USDT markets
        print("\n⚠️  WARNING: This will fetch data for ALL USDT markets on CoinEx")
        print("   This may take a significant amount of time (10-30 minutes)")
        print("   Depending on the number of markets (~300-400 USDT pairs)\n")
        
        days_input = input(f"Enter number of days (default: {DAYS}, max: 1000): ").strip()
        days = int(days_input) if days_input else DAYS
        
        filename_input = input("Output filename (default: all_usdt_markets_daily.csv): ").strip()
        filename = filename_input if filename_input else "all_usdt_markets_daily.csv"
        
        fetcher.fetch_all_usdt_markets(days, filename)
    
    elif choice == "6":
        # Trend analysis
        print("\n📈 MANN-KENDALL TREND ANALYSIS")
        print("   This will analyze ALL USDT markets for trend strength")
        print("   Helps identify markets with strongest uptrends/downtrends\n")
        
        days_input = input(f"Enter number of days to analyze (default: {DAYS}, max: 1000): ").strip()
        days = int(days_input) if days_input else DAYS
        
        top_n_input = input("How many top trends to display? (default: 20): ").strip()
        top_n = int(top_n_input) if top_n_input else 20
        
        # Run analysis
        results = fetcher.analyze_all_usdt_trends(days)
        
        if results:
            # Display results
            fetcher.display_trend_results(results, top_n)
            
            # Offer to save
            save = input("Save full results to CSV? (y/n): ").strip().lower()
            if save == 'y':
                filename = input("Filename (default: trend_analysis_results.csv): ").strip()
                if not filename:
                    filename = "trend_analysis_results.csv"
                fetcher.save_trend_results(results, filename)
        
    else:
        print("❌ Invalid choice")


if __name__ == "__main__":
    main()
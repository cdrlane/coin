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
    
    def analyze_trend(self, data: List[Dict], use_modified: bool = True) -> Dict:
        """
        Analyze trend using Mann-Kendall test
        
        Args:
            data: Parsed kline data with Close prices
            use_modified: If True, use Hamed-Rao modified test (better for autocorrelated data)
            
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
                'z_score': None,
                'test_type': 'none'
            }
        
        if not data or len(data) < 3:
            return {
                'error': 'insufficient data',
                'trend': 'unknown',
                'p_value': None,
                'tau': None,
                'slope': None,
                'z_score': None,
                'test_type': 'none'
            }
        
        try:
            # Extract close prices
            close_prices = [float(d['Close']) for d in data]
            
            # Run appropriate Mann-Kendall test
            if use_modified:
                # Hamed-Rao Modified Test - accounts for autocorrelation
                # Better for financial time series
                result = mk.hamed_rao_modification_test(close_prices)
                test_type = 'Hamed-Rao Modified'
            else:
                # Original Mann-Kendall test
                result = mk.original_test(close_prices)
                test_type = 'Original'
            
            # Check what attributes are available
            # Different versions may have different attribute names
            tau = getattr(result, 'tau', getattr(result, 'Tau', None))
            slope = getattr(result, 'slope', getattr(result, 'Sen_slope', None))
            
            return {
                'trend': result.trend,           # 'increasing', 'decreasing', 'no trend'
                'p_value': result.p,             # Statistical significance
                'tau': tau,                      # Kendall's tau (-1 to 1)
                'slope': slope,                  # Sen's slope (rate of change)
                'z_score': result.z,             # Z-score
                'h': result.h,                   # True if trend is significant
                'significance': 'significant' if result.h else 'not significant',
                'test_type': test_type
            }
        except Exception as e:
            import traceback
            return {
                'error': f"{str(e)} - {traceback.format_exc()[:200]}",
                'trend': 'error',
                'p_value': None,
                'tau': None,
                'slope': None,
                'z_score': None,
                'test_type': 'error'
            }
    
    def analyze_all_usdt_trends(self, days: int = 365, min_data_points: int = 30, use_modified: bool = True):
        """
        Analyze trends for all USDT markets and rank by trend strength
        
        Args:
            days: Number of days to analyze
            min_data_points: Minimum data points required for analysis
            use_modified: If True, use Hamed-Rao modified test (recommended for crypto)
            
        Returns:
            List of markets sorted by trend strength
        """
        print(f"\n{'='*100}")
        print(f"MANN-KENDALL TREND ANALYSIS - ALL USDT MARKETS")
        print(f"Test Type: {'Hamed-Rao Modified (accounts for autocorrelation)' if use_modified else 'Original Mann-Kendall'}")
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
        print(f"Analyzing {days} days of data")
        print(f"Minimum data points required: {min_data_points}\n")
        
        proceed = input(f"Analyze {len(usdt_markets)} markets? (y/n): ").strip().lower()
        if proceed != 'y':
            return []
        
        results = []
        fetch_failed = 0
        parse_failed = 0
        insufficient_data = 0
        trend_error = 0
        
        for i, market in enumerate(usdt_markets, 1):
            if i % 50 == 0 or i <= 5:
                print(f"Progress: {i}/{len(usdt_markets)} markets analyzed... (Success: {len(results)})")
            
            try:
                # Fetch data (silent mode to reduce output)
                klines = self.get_daily_klines(market, limit=days, silent=True)
                if not klines:
                    if i <= 5:
                        print(f"  ⚠️  {market}: No klines returned")
                    fetch_failed += 1
                    continue
                
                # Parse data
                parsed_data = self.parse_kline_data(klines)
                if not parsed_data:
                    if i <= 5:
                        print(f"  ⚠️  {market}: Parse failed")
                    parse_failed += 1
                    continue
                
                if len(parsed_data) < min_data_points:
                    if i <= 5:
                        print(f"  ⚠️  {market}: Only {len(parsed_data)} data points (need {min_data_points})")
                    insufficient_data += 1
                    continue
                
                # Analyze trend with selected test type
                trend_result = self.analyze_trend(parsed_data, use_modified=use_modified)
                
                if trend_result.get('trend') == 'error':
                    if i <= 5:
                        print(f"  ⚠️  {market}: Trend analysis error - {trend_result.get('error')}")
                    trend_error += 1
                    continue
                
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
                    'test_type': trend_result.get('test_type', 'unknown'),
                    'data_points': len(parsed_data),
                    'first_close': first_close,
                    'last_close': last_close,
                    'total_change_%': total_change_pct
                })
                
                if i <= 5:
                    print(f"  ✓ {market}: {len(parsed_data)} points, trend={trend_result['trend']}, tau={trend_result['tau']:.4f}")
                
                # Rate limiting
                time.sleep(0.3)
                
            except Exception as e:
                if i <= 5:
                    print(f"  ❌ {market}: Exception - {e}")
                trend_error += 1
                continue
        
        print(f"\n✓ Analysis complete!")
        print(f"   Successfully analyzed: {len(results)}")
        print(f"   Fetch failed:          {fetch_failed}")
        print(f"   Parse failed:          {parse_failed}")
        print(f"   Insufficient data:     {insufficient_data}")
        print(f"   Trend error:           {trend_error}")
        print()
        
        return results
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
        print(f"Analyzing {days} days of data")
        print(f"Minimum data points required: {min_data_points}\n")
        
        proceed = input(f"Analyze {len(usdt_markets)} markets? (y/n): ").strip().lower()
        if proceed != 'y':
            return []
        
        results = []
        fetch_failed = 0
        parse_failed = 0
        insufficient_data = 0
        trend_error = 0
        
        for i, market in enumerate(usdt_markets, 1):
            if i % 50 == 0 or i <= 5:
                print(f"Progress: {i}/{len(usdt_markets)} markets analyzed... (Success: {len(results)})")
            
            try:
                # Fetch data (silent mode to reduce output)
                klines = self.get_daily_klines(market, limit=days, silent=True)
                if not klines:
                    if i <= 5:
                        print(f"  ⚠️  {market}: No klines returned")
                    fetch_failed += 1
                    continue
                
                # Parse data
                parsed_data = self.parse_kline_data(klines)
                if not parsed_data:
                    if i <= 5:
                        print(f"  ⚠️  {market}: Parse failed")
                    parse_failed += 1
                    continue
                
                if len(parsed_data) < min_data_points:
                    if i <= 5:
                        print(f"  ⚠️  {market}: Only {len(parsed_data)} data points (need {min_data_points})")
                    insufficient_data += 1
                    continue
                
                # Analyze trend
                trend_result = self.analyze_trend(parsed_data)
                
                if trend_result.get('trend') == 'error':
                    if i <= 5:
                        print(f"  ⚠️  {market}: Trend analysis error - {trend_result.get('error')}")
                    trend_error += 1
                    continue
                
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
                
                if i <= 5:
                    print(f"  ✓ {market}: {len(parsed_data)} points, trend={trend_result['trend']}, tau={trend_result['tau']:.4f}")
                
                # Rate limiting
                time.sleep(0.3)
                
            except Exception as e:
                if i <= 5:
                    print(f"  ❌ {market}: Exception - {e}")
                trend_error += 1
                continue
        
        print(f"\n✓ Analysis complete!")
        print(f"   Successfully analyzed: {len(results)}")
        print(f"   Fetch failed:          {fetch_failed}")
        print(f"   Parse failed:          {parse_failed}")
        print(f"   Insufficient data:     {insufficient_data}")
        print(f"   Trend error:           {trend_error}")
        print()
        
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
    
    def analyze_trend_rolling_window(self, data: List[Dict], window_size: int = 30, 
                                    use_modified: bool = True) -> List[Dict]:
        """
        Analyze trend using rolling window approach
        
        Args:
            data: Parsed kline data with Close prices
            window_size: Size of rolling window (default: 30 days)
            use_modified: If True, use Hamed-Rao modified test
            
        Returns:
            List of trend results for each window
        """
        if not HAS_MK:
            return []
        
        if not data or len(data) < window_size:
            return []
        
        rolling_results = []
        
        # Slide window through data
        for i in range(len(data) - window_size + 1):
            window_data = data[i:i + window_size]
            
            # Analyze this window
            trend_result = self.analyze_trend(window_data, use_modified=use_modified)
            
            if trend_result.get('trend') != 'error':
                # Add window metadata
                result_with_meta = trend_result.copy()
                result_with_meta['window_start'] = window_data[0]['Date']
                result_with_meta['window_end'] = window_data[-1]['Date']
                result_with_meta['window_size'] = window_size
                result_with_meta['window_index'] = i
                
                rolling_results.append(result_with_meta)
        
        return rolling_results
    
    def analyze_rolling_window_all_markets(self, days: int = 90, window_size: int = 30, 
                                          use_modified: bool = True):
        """
        Analyze ALL USDT markets using rolling window approach
        
        Args:
            days: Total days of historical data to fetch
            window_size: Size of rolling window (e.g., 30 days)
            use_modified: If True, use Hamed-Rao modified test
            
        Returns:
            Dictionary with market results
        """
        print(f"\n{'='*100}")
        print(f"ROLLING WINDOW TREND ANALYSIS - ALL USDT MARKETS")
        print(f"Analysis Period: Last {days} days")
        print(f"Window Size: {window_size} days")
        print(f"Test Type: {'Hamed-Rao Modified' if use_modified else 'Original Mann-Kendall'}")
        print(f"{'='*100}\n")
        
        if not HAS_MK:
            print("❌ pymannkendall not installed")
            return {}
        
        if window_size >= days:
            print(f"❌ Window size ({window_size}) must be smaller than total days ({days})")
            return {}
        
        # Get all markets
        all_markets = self.get_all_markets()
        if not all_markets:
            return {}
        
        # Filter for USDT markets
        usdt_markets = [m for m in all_markets if m.endswith('USDT')]
        
        num_windows = days - window_size + 1
        print(f"Found {len(usdt_markets)} USDT markets")
        print(f"Each market will have {num_windows} rolling windows analyzed\n")
        
        proceed = input(f"Analyze {len(usdt_markets)} markets with rolling windows? (y/n): ").strip().lower()
        if proceed != 'y':
            return {}
        
        market_results = {}
        successful = 0
        failed = 0
        
        for i, market in enumerate(usdt_markets, 1):
            if i % 50 == 0 or i <= 3:
                print(f"Progress: {i}/{len(usdt_markets)} markets... (Success: {successful})")
            
            try:
                # Fetch data
                klines = self.get_daily_klines(market, limit=days, silent=True)
                if not klines:
                    failed += 1
                    continue
                
                # Parse data
                parsed_data = self.parse_kline_data(klines)
                if not parsed_data or len(parsed_data) < days:
                    failed += 1
                    continue
                
                # Analyze with rolling window
                rolling_results = self.analyze_trend_rolling_window(
                    parsed_data, 
                    window_size=window_size, 
                    use_modified=use_modified
                )
                
                if rolling_results:
                    # Calculate aggregate metrics
                    trends = [r['trend'] for r in rolling_results]
                    taus = [r['tau'] for r in rolling_results if r['tau'] is not None]
                    
                    # Most recent window (most important)
                    latest_window = rolling_results[-1]
                    
                    # Trend consistency
                    increasing_count = trends.count('increasing')
                    decreasing_count = trends.count('decreasing')
                    no_trend_count = trends.count('no trend')
                    
                    # Average tau across all windows
                    avg_tau = sum(taus) / len(taus) if taus else 0
                    
                    market_results[market] = {
                        'market': market,
                        'total_windows': len(rolling_results),
                        'latest_trend': latest_window['trend'],
                        'latest_tau': latest_window['tau'],
                        'latest_p_value': latest_window['p_value'],
                        'latest_slope': latest_window['slope'],
                        'latest_significance': latest_window['significance'],
                        'increasing_windows': increasing_count,
                        'decreasing_windows': decreasing_count,
                        'no_trend_windows': no_trend_count,
                        'avg_tau': avg_tau,
                        'trend_consistency': max(increasing_count, decreasing_count) / len(rolling_results) * 100,
                        'all_windows': rolling_results
                    }
                    
                    successful += 1
                    
                    if i <= 3:
                        print(f"  ✓ {market}: {len(rolling_results)} windows, latest={latest_window['trend']}")
                else:
                    failed += 1
                
                # Rate limiting
                time.sleep(0.3)
                
            except Exception as e:
                if i <= 3:
                    print(f"  ❌ {market}: {e}")
                failed += 1
                continue
        
        print(f"\n✓ Rolling window analysis complete!")
        print(f"   Successfully analyzed: {successful}")
        print(f"   Failed:                {failed}\n")
        
        return market_results
    
    def display_rolling_results(self, market_results: Dict, top_n: int = 20):
        """Display rolling window analysis results"""
        if not market_results:
            print("No results to display")
            return
        
        # Convert to list for sorting
        results_list = list(market_results.values())
        
        print(f"\n{'='*100}")
        print(f"ROLLING WINDOW ANALYSIS SUMMARY")
        print(f"{'='*100}")
        print(f"Total markets analyzed: {len(results_list)}")
        
        # Count latest trends
        latest_increasing = sum(1 for r in results_list if r['latest_trend'] == 'increasing')
        latest_decreasing = sum(1 for r in results_list if r['latest_trend'] == 'decreasing')
        latest_no_trend = sum(1 for r in results_list if r['latest_trend'] == 'no trend')
        
        print(f"Latest window trends:")
        print(f"  Increasing: {latest_increasing} ({latest_increasing/len(results_list)*100:.1f}%)")
        print(f"  Decreasing: {latest_decreasing} ({latest_decreasing/len(results_list)*100:.1f}%)")
        print(f"  No trend:   {latest_no_trend} ({latest_no_trend/len(results_list)*100:.1f}%)")
        print(f"{'='*100}\n")
        
        # Top consistent uptrends
        consistent_up = [r for r in results_list if r['latest_trend'] == 'increasing']
        consistent_up.sort(key=lambda x: x['trend_consistency'], reverse=True)
        
        if consistent_up:
            print(f"\n{'='*100}")
            print(f"TOP {min(top_n, len(consistent_up))} MOST CONSISTENT UPTRENDS")
            print(f"{'='*100}")
            print(f"{'Market':<15} {'Latest Tau':<12} {'Avg Tau':<12} {'Consistency':<12} "
                  f"{'Inc/Total':<15} {'Latest Sig.':<12}")
            print("-"*100)
            
            for r in consistent_up[:top_n]:
                print(f"{r['market']:<15} {r['latest_tau']:>11.4f} {r['avg_tau']:>11.4f} "
                      f"{r['trend_consistency']:>10.1f}% "
                      f"{r['increasing_windows']:>3}/{r['total_windows']:<9} "
                      f"{r['latest_significance']:<12}")
        
        # Top consistent downtrends
        consistent_down = [r for r in results_list if r['latest_trend'] == 'decreasing']
        consistent_down.sort(key=lambda x: x['trend_consistency'], reverse=True)
        
        if consistent_down:
            print(f"\n{'='*100}")
            print(f"TOP {min(top_n, len(consistent_down))} MOST CONSISTENT DOWNTRENDS")
            print(f"{'='*100}")
            print(f"{'Market':<15} {'Latest Tau':<12} {'Avg Tau':<12} {'Consistency':<12} "
                  f"{'Dec/Total':<15} {'Latest Sig.':<12}")
            print("-"*100)
            
            for r in consistent_down[:top_n]:
                print(f"{r['market']:<15} {r['latest_tau']:>11.4f} {r['avg_tau']:>11.4f} "
                      f"{r['trend_consistency']:>10.1f}% "
                      f"{r['decreasing_windows']:>3}/{r['total_windows']:<9} "
                      f"{r['latest_significance']:<12}")
        
        print(f"\n{'='*100}\n")
    
    def save_rolling_results(self, market_results: Dict, filename: str = "rolling_trend_analysis.csv"):
        """Save rolling window analysis results to CSV"""
        if not market_results:
            print("No results to save")
            return
        
        try:
            results_list = list(market_results.values())
            
            with open(filename, 'w', newline='') as f:
                fieldnames = ['market', 'total_windows', 'latest_trend', 'latest_tau', 
                            'latest_p_value', 'latest_slope', 'latest_significance',
                            'increasing_windows', 'decreasing_windows', 'no_trend_windows',
                            'avg_tau', 'trend_consistency']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for result in results_list:
                    # Remove the nested 'all_windows' data for CSV
                    csv_row = {k: v for k, v in result.items() if k != 'all_windows'}
                    writer.writerow(csv_row)
            
            print(f"✓ Saved {len(results_list)} rolling analysis results to {filename}")
        except Exception as e:
            print(f"❌ Error saving results: {e}")
    
    def save_window_by_window_analysis(self, market_results: Dict, 
                                      filename: str = "window_by_window_trends.csv",
                                      top_n_per_window: int = 10):
        """
        Save detailed window-by-window analysis showing top trends for each window
        
        Args:
            market_results: Results from analyze_rolling_window_all_markets
            filename: Output CSV filename
            top_n_per_window: Number of top up/down trends to save per window
        """
        if not market_results:
            print("No results to save")
            return
        
        try:
            import os
            
            # Get the number of windows (should be same for all markets)
            first_market = next(iter(market_results.values()))
            num_windows = first_market['total_windows']
            
            print(f"\n{'='*80}")
            print(f"Generating window-by-window analysis for {num_windows} windows...")
            print(f"{'='*80}")
            
            # For each window, collect all market trends
            window_data = []
            
            print(f"Collecting data for {num_windows} windows...")
            
            for window_idx in range(num_windows):
                # Collect all markets' data for this specific window
                window_markets = []
                
                for market, result in market_results.items():
                    if window_idx < len(result['all_windows']):
                        window_info = result['all_windows'][window_idx]
                        window_markets.append({
                            'market': market,
                            'window_index': window_idx,
                            'window_start': window_info['window_start'],
                            'window_end': window_info['window_end'],
                            'trend': window_info['trend'],
                            'tau': window_info['tau'],
                            'p_value': window_info['p_value'],
                            'slope': window_info['slope'],
                            'z_score': window_info['z_score'],
                            'significance': window_info['significance']
                        })
                
                # Sort by tau (strongest trends first)
                window_markets.sort(key=lambda x: abs(x['tau']) if x['tau'] is not None else 0, reverse=True)
                
                # Get top uptrends
                uptrends = [m for m in window_markets if m['trend'] == 'increasing'][:top_n_per_window]
                
                # Get top downtrends
                downtrends = [m for m in window_markets if m['trend'] == 'decreasing'][:top_n_per_window]
                downtrends.sort(key=lambda x: x['tau'] if x['tau'] is not None else 0)  # Most negative first
                
                # Store this window's data
                window_data.append({
                    'window_index': window_idx,
                    'window_start': window_markets[0]['window_start'] if window_markets else '',
                    'window_end': window_markets[0]['window_end'] if window_markets else '',
                    'top_uptrends': uptrends,
                    'top_downtrends': downtrends
                })
                
                # Progress indicator
                if (window_idx + 1) % 10 == 0:
                    print(f"  Processed {window_idx + 1}/{num_windows} windows...")
            
            print(f"✓ Collected data for {len(window_data)} windows")
            print(f"  Writing to CSV...")
            
            # Write to CSV
            with open(filename, 'w', newline='') as f:
                fieldnames = ['window_index', 'window_start', 'window_end', 'rank', 'direction',
                            'market', 'trend', 'tau', 'p_value', 'slope', 'z_score', 'significance']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                rows_written = 0
                
                for window in window_data:
                    # Debug: Show what we're processing
                    if window['window_index'] < 3 or window['window_index'] >= num_windows - 1:
                        print(f"  Window {window['window_index']}: {len(window['top_uptrends'])} uptrends, {len(window['top_downtrends'])} downtrends")
                    
                    # Write uptrends
                    for rank, market_data in enumerate(window['top_uptrends'], 1):
                        row = {
                            'window_index': window['window_index'],
                            'window_start': window['window_start'],
                            'window_end': window['window_end'],
                            'rank': rank,
                            'direction': 'UP',
                            'market': market_data['market'],
                            'trend': market_data['trend'],
                            'tau': market_data['tau'],
                            'p_value': market_data['p_value'],
                            'slope': market_data['slope'],
                            'z_score': market_data['z_score'],
                            'significance': market_data['significance']
                        }
                        writer.writerow(row)
                        rows_written += 1
                    
                    # Write downtrends
                    for rank, market_data in enumerate(window['top_downtrends'], 1):
                        row = {
                            'window_index': window['window_index'],
                            'window_start': window['window_start'],
                            'window_end': window['window_end'],
                            'rank': rank,
                            'direction': 'DOWN',
                            'market': market_data['market'],
                            'trend': market_data['trend'],
                            'tau': market_data['tau'],
                            'p_value': market_data['p_value'],
                            'slope': market_data['slope'],
                            'z_score': market_data['z_score'],
                            'significance': market_data['significance']
                        }
                        writer.writerow(row)
                        rows_written += 1
            
            # Get absolute path
            abs_path = os.path.abspath(filename)
            
            print(f"\n{'='*80}")
            print(f"✓ SUCCESSFULLY SAVED WINDOW-BY-WINDOW ANALYSIS")
            print(f"{'='*80}")
            print(f"Filename:          {filename}")
            print(f"Full path:         {abs_path}")
            print(f"Total windows:     {num_windows}")
            print(f"Top per window:    {top_n_per_window} up + {top_n_per_window} down")
            print(f"Total rows:        {rows_written}")
            print(f"File size:         {os.path.getsize(filename):,} bytes")
            print(f"{'='*80}\n")
            
        except Exception as e:
            print(f"❌ Error saving window-by-window analysis: {e}")
            import traceback
            traceback.print_exc()
    
    def analyze_rolling_window_from_csv(self, csv_file: str, window_size: int = 30, 
                                       use_modified: bool = True, min_data_points: int = 0):
        """
        Analyze rolling window trends from a pre-existing CSV file
        
        Args:
            csv_file: Path to CSV file with market data (from option 5 or 2)
            window_size: Size of rolling window
            use_modified: If True, use Hamed-Rao modified test
            min_data_points: Minimum data points required (0 = only check window_size)
            
        Returns:
            Dictionary with market results
        """
        print(f"\n{'='*100}")
        print(f"ROLLING WINDOW ANALYSIS FROM CSV FILE")
        print(f"CSV File: {csv_file}")
        print(f"Window Size: {window_size} days")
        print(f"Test Type: {'Hamed-Rao Modified' if use_modified else 'Original Mann-Kendall'}")
        print(f"{'='*100}\n")
        
        if not HAS_MK:
            print("❌ pymannkendall not installed")
            return {}
        
        import os
        if not os.path.exists(csv_file):
            print(f"❌ CSV file not found: {csv_file}")
            return {}
        
        try:
            # Load data from CSV
            print(f"Loading data from {csv_file}...")
            
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                all_rows = list(reader)
            
            print(f"✓ Loaded {len(all_rows)} rows from CSV")
            
            # Group by market
            markets_data = {}
            for row in all_rows:
                market = row.get('Market', row.get('market', ''))
                if not market:
                    continue
                
                if market not in markets_data:
                    markets_data[market] = []
                
                # Convert to format expected by analyze_trend
                markets_data[market].append({
                    'Date': row.get('Date', ''),
                    'Timestamp': row.get('Timestamp', ''),
                    'Close': float(row.get('Close', row.get('close', 0))),
                    'Open': float(row.get('Open', row.get('open', 0))),
                    'High': float(row.get('High', row.get('high', 0))),
                    'Low': float(row.get('Low', row.get('low', 0))),
                    'Volume': float(row.get('Volume', row.get('volume', 0)))
                })
            
            # Filter for USDT markets
            usdt_markets = {k: v for k, v in markets_data.items() if k.endswith('USDT')}
            
            print(f"✓ Found {len(usdt_markets)} USDT markets in CSV")
            
            # Show data summary
            if usdt_markets:
                data_lengths = [len(v) for v in usdt_markets.values()]
                print(f"   Data points per market:")
                print(f"      Min:     {min(data_lengths)} days")
                print(f"      Max:     {max(data_lengths)} days")
                print(f"      Average: {sum(data_lengths)/len(data_lengths):.1f} days")
                print()
            
            # Check if we have enough data
            if min_data_points > 0:
                # Filter by minimum data points first
                markets_with_enough_data = {k: v for k, v in usdt_markets.items() 
                                           if len(v) >= min_data_points}
                
                if not markets_with_enough_data:
                    print(f"❌ No markets have enough data (need at least {min_data_points} days)")
                    return {}
                
                filtered_count = len(usdt_markets) - len(markets_with_enough_data)
                if filtered_count > 0:
                    print(f"   Filtered out {filtered_count} markets with < {min_data_points} data points")
            else:
                # Just check window size
                markets_with_enough_data = {k: v for k, v in usdt_markets.items() 
                                           if len(v) >= window_size}
                
                if not markets_with_enough_data:
                    print(f"❌ No markets have enough data (need at least {window_size} days)")
                    return {}
            
            print(f"✓ {len(markets_with_enough_data)} markets will be analyzed\n")
            
            # Determine analysis period
            total_days = min(len(v) for v in markets_with_enough_data.values())
            num_windows = total_days - window_size + 1
            
            print(f"📊 DATA ANALYSIS:")
            print(f"   Markets with enough data: {len(markets_with_enough_data)}")
            print(f"   Shortest market data:     {total_days} days")
            print(f"   Window size:              {window_size} days")
            print(f"   Calculated windows:       {num_windows}")
            print()
            
            if num_windows < 1:
                print(f"❌ Cannot create any windows!")
                print(f"   Total days ({total_days}) must be >= window size ({window_size})")
                return {}
            
            if num_windows < 5:
                print(f"⚠️  WARNING: Only {num_windows} window(s) will be created")
                print(f"   For meaningful rolling analysis, you need more data:")
                print(f"   - Current: {total_days} days")
                print(f"   - Recommended: {window_size * 3}+ days for {window_size}-day windows")
                print()
            
            # Show data range for first few markets
            print(f"📅 DATA RANGE SAMPLE:")
            for i, (market, data) in enumerate(list(markets_with_enough_data.items())[:3]):
                sorted_data = sorted(data, key=lambda x: x['Date'])
                print(f"   {market}: {sorted_data[0]['Date']} to {sorted_data[-1]['Date']} ({len(data)} days)")
            print()
            
            proceed = input(f"Analyze {len(markets_with_enough_data)} markets? (y/n): ").strip().lower()
            if proceed != 'y':
                return {}
            
            market_results = {}
            successful = 0
            failed = 0
            
            for i, (market, data) in enumerate(markets_with_enough_data.items(), 1):
                if i % 50 == 0 or i <= 3:
                    print(f"Progress: {i}/{len(markets_with_enough_data)} markets... (Success: {successful})")
                
                try:
                    # Sort by date to ensure chronological order
                    data_sorted = sorted(data, key=lambda x: x['Date'])
                    
                    # Use only the period we're analyzing
                    data_to_analyze = data_sorted[-total_days:]
                    
                    # Analyze with rolling window
                    rolling_results = self.analyze_trend_rolling_window(
                        data_to_analyze,
                        window_size=window_size,
                        use_modified=use_modified
                    )
                    
                    if rolling_results:
                        # Calculate aggregate metrics
                        trends = [r['trend'] for r in rolling_results]
                        taus = [r['tau'] for r in rolling_results if r['tau'] is not None]
                        
                        # Most recent window
                        latest_window = rolling_results[-1]
                        
                        # Trend consistency
                        increasing_count = trends.count('increasing')
                        decreasing_count = trends.count('decreasing')
                        no_trend_count = trends.count('no trend')
                        
                        # Average tau
                        avg_tau = sum(taus) / len(taus) if taus else 0
                        
                        market_results[market] = {
                            'market': market,
                            'total_windows': len(rolling_results),
                            'latest_trend': latest_window['trend'],
                            'latest_tau': latest_window['tau'],
                            'latest_p_value': latest_window['p_value'],
                            'latest_slope': latest_window['slope'],
                            'latest_significance': latest_window['significance'],
                            'increasing_windows': increasing_count,
                            'decreasing_windows': decreasing_count,
                            'no_trend_windows': no_trend_count,
                            'avg_tau': avg_tau,
                            'trend_consistency': max(increasing_count, decreasing_count) / len(rolling_results) * 100,
                            'all_windows': rolling_results
                        }
                        
                        successful += 1
                        
                        if i <= 3:
                            print(f"  ✓ {market}: {len(rolling_results)} windows, latest={latest_window['trend']}")
                    else:
                        failed += 1
                        
                except Exception as e:
                    if i <= 3:
                        print(f"  ❌ {market}: {e}")
                    failed += 1
                    continue
            
            print(f"\n✓ Rolling window analysis complete!")
            print(f"   Successfully analyzed: {successful}")
            print(f"   Failed:                {failed}\n")
            
            return market_results
            
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            import traceback
            traceback.print_exc()
            return {}
    
    def save_trend_results(self, results: List[Dict], filename: str = "trend_analysis_results.csv"):
        """Save rolling window analysis results to CSV"""
        if not market_results:
            print("No results to save")
            return
        
        try:
            results_list = list(market_results.values())
            
            with open(filename, 'w', newline='') as f:
                fieldnames = ['market', 'total_windows', 'latest_trend', 'latest_tau', 
                            'latest_p_value', 'latest_slope', 'latest_significance',
                            'increasing_windows', 'decreasing_windows', 'no_trend_windows',
                            'avg_tau', 'trend_consistency']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for result in results_list:
                    # Remove the nested 'all_windows' data for CSV
                    csv_row = {k: v for k, v in result.items() if k != 'all_windows'}
                    writer.writerow(csv_row)
            
            print(f"✓ Saved {len(results_list)} rolling analysis results to {filename}")
        except Exception as e:
            print(f"❌ Error saving results: {e}")
        """Save trend analysis results to CSV"""
        if not results:
            print("No results to save")
            return
        
        try:
            with open(filename, 'w', newline='') as f:
                fieldnames = ['market', 'trend', 'p_value', 'tau', 'slope', 'z_score', 
                            'significance', 'test_type', 'data_points', 'first_close', 
                            'last_close', 'total_change_%']
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
        
    def get_daily_klines(self, market: str, limit: int = 1000, silent: bool = False) -> List[Dict]:
        """
        Get daily candlestick data for a market
        
        Args:
            market: Market symbol (e.g., 'BTCUSDT')
            limit: Number of days to fetch (max 1000)
            silent: If True, suppress output messages
            
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
            
            if not silent:
                print(f"Fetching {limit} days of data for {market}...")
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get("code") == 0:
                    klines = data.get("data", [])
                    if not silent:
                        print(f"✓ Retrieved {len(klines)} daily candles")
                    
                    # Only show debug for first market when not silent
                    if not silent and klines and len(klines) > 0:
                        print(f"\n📊 Sample kline structure:")
                        print(f"   Type: {type(klines[0])}")
                        print(f"   Length: {len(klines[0]) if isinstance(klines[0], (list, tuple)) else 'N/A'}")
                        print(f"   First kline: {klines[0]}")
                        print()
                    
                    return klines
                else:
                    if not silent:
                        print(f"❌ API Error: {data.get('message')}")
                    return []
            else:
                if not silent:
                    print(f"❌ HTTP Error: {response.status_code}")
                return []
                
        except Exception as e:
            if not silent:
                print(f"❌ Error fetching data: {e}")
                import traceback
                traceback.print_exc()
            return []
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
    
    def fetch_all_usdt_markets(self, days: int = 365, filename: str = "all_usdt_markets_daily.csv",
                              min_data_points: int = 0):
        """
        Fetch data for ALL USDT markets on CoinEx
        
        Args:
            days: Number of days to fetch
            filename: Output CSV filename
            min_data_points: Minimum data points required (0 = no filter, markets with fewer points excluded)
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
        if min_data_points > 0:
            print(f"Filter: Only keeping markets with {min_data_points}+ data points")
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
        filtered_out = 0
        
        for i, market in enumerate(usdt_markets, 1):
            print(f"\n[{i}/{len(usdt_markets)}] Processing {market}...")
            
            try:
                klines = self.get_daily_klines(market, limit=days)
                
                if klines:
                    parsed_data = self.parse_kline_data(klines)
                    
                    if parsed_data:
                        # Check minimum data points
                        if min_data_points > 0 and len(parsed_data) < min_data_points:
                            print(f"   ⚠️  Filtered: Only {len(parsed_data)} data points (need {min_data_points})")
                            filtered_out += 1
                            continue
                        
                        # Append to single file (only if we have successful markets already or this is first)
                        self.save_to_csv(parsed_data, filename, append=(successful > 0))
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
                print(f"    Successful: {successful} | Failed: {failed} | Filtered: {filtered_out} | Total records: {total_records}\n")
        
        print(f"\n{'='*80}")
        print(f"✓ Completed! All USDT markets data saved to: {filename}")
        print(f"   Total markets attempted: {len(usdt_markets)}")
        print(f"   Successful:              {successful}")
        print(f"   Failed:                  {failed}")
        if min_data_points > 0:
            print(f"   Filtered out:            {filtered_out} (< {min_data_points} data points)")
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
    print("7. Rolling window trend analysis (e.g., 30-day windows over 90 days)")
    print("8. Rolling window analysis FROM CSV (faster - no fetching)")
    print()
    
    choice = input("Enter choice (1-8): ").strip()
    
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
        
        min_points_input = input(f"Minimum data points required (default: 0 = no filter): ").strip()
        min_points = int(min_points_input) if min_points_input else 0
        
        filename_input = input("Output filename (default: all_usdt_markets_daily.csv): ").strip()
        filename = filename_input if filename_input else "all_usdt_markets_daily.csv"
        
        fetcher.fetch_all_usdt_markets(days, filename, min_points)
    
    elif choice == "6":
        # Trend analysis
        print("\n📈 MANN-KENDALL TREND ANALYSIS")
        print("   This will analyze ALL USDT markets for trend strength")
        print("   Helps identify markets with strongest uptrends/downtrends\n")
        
        print("Select test type:")
        print("1. Hamed-Rao Modified (RECOMMENDED for crypto - handles autocorrelation)")
        print("2. Original Mann-Kendall")
        test_choice = input("Test type (1 or 2, default: 1): ").strip()
        use_modified = test_choice != "2"
        
        days_input = input(f"\nEnter number of days to analyze (default: {DAYS}, max: 1000): ").strip()
        days = int(days_input) if days_input else DAYS
        
        top_n_input = input("How many top trends to display? (default: 20): ").strip()
        top_n = int(top_n_input) if top_n_input else 20
        
        # Run analysis
        results = fetcher.analyze_all_usdt_trends(days, use_modified=use_modified)
        
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
    
    elif choice == "7":
        # Rolling window analysis
        print("\n📊 ROLLING WINDOW TREND ANALYSIS")
        print("   Analyze trends using a sliding window approach")
        print("   Example: 30-day window sliding over 90 days = 61 trend measurements\n")
        
        print("Select test type:")
        print("1. Hamed-Rao Modified (RECOMMENDED for crypto)")
        print("2. Original Mann-Kendall")
        test_choice = input("Test type (1 or 2, default: 1): ").strip()
        use_modified = test_choice != "2"
        
        days_input = input("\nTotal days of historical data (default: 90): ").strip()
        days = int(days_input) if days_input else 90
        
        window_input = input("Window size in days (default: 30): ").strip()
        window_size = int(window_input) if window_input else 30
        
        if window_size >= days:
            print(f"❌ Window size ({window_size}) must be smaller than total days ({days})")
        else:
            top_n_input = input("How many top results to display? (default: 20): ").strip()
            top_n = int(top_n_input) if top_n_input else 20
            
            # Run rolling window analysis
            results = fetcher.analyze_rolling_window_all_markets(
                days=days, 
                window_size=window_size, 
                use_modified=use_modified
            )
            
            if results:
                # Display results
                fetcher.display_rolling_results(results, top_n)
                
                # Offer to save summary
                save = input("\nSave summary results to CSV? (y/n): ").strip().lower()
                if save == 'y':
                    filename = input("Filename (default: rolling_trend_analysis.csv): ").strip()
                    if not filename:
                        filename = "rolling_trend_analysis.csv"
                    fetcher.save_rolling_results(results, filename)
                
                # Offer to save detailed window-by-window analysis
                save_detailed = input("\nSave detailed window-by-window analysis? (y/n): ").strip().lower()
                if save_detailed == 'y':
                    filename = input("Filename (default: window_by_window_trends.csv): ").strip()
                    if not filename:
                        filename = "window_by_window_trends.csv"
                    
                    top_n_input = input("Top N markets per window (default: 10): ").strip()
                    top_n_per_window = int(top_n_input) if top_n_input else 10
                    
                    fetcher.save_window_by_window_analysis(results, filename, top_n_per_window)
    
    elif choice == "8":
        # Rolling window analysis from CSV
        print("\n📊 ROLLING WINDOW ANALYSIS FROM CSV")
        print("   Analyze pre-fetched data (much faster!)")
        print("   Use CSV files from option 2 or option 5\n")
        
        csv_file = input("Enter CSV filename (e.g., all_markets_daily.csv): ").strip()
        
        if not csv_file:
            print("❌ No filename provided")
        else:
            print("\nSelect test type:")
            print("1. Hamed-Rao Modified (RECOMMENDED for crypto)")
            print("2. Original Mann-Kendall")
            test_choice = input("Test type (1 or 2, default: 1): ").strip()
            use_modified = test_choice != "2"
            
            window_input = input("\nWindow size in days (default: 30): ").strip()
            window_size = int(window_input) if window_input else 30
            
            min_points_input = input("Minimum data points per market (default: 0 = only check window size): ").strip()
            min_points = int(min_points_input) if min_points_input else 0
            
            top_n_input = input("How many top results to display? (default: 20): ").strip()
            top_n = int(top_n_input) if top_n_input else 20
            
            # Run rolling window analysis from CSV
            results = fetcher.analyze_rolling_window_from_csv(
                csv_file=csv_file,
                window_size=window_size,
                use_modified=use_modified,
                min_data_points=min_points
            )
            
            if results:
                # Display results
                fetcher.display_rolling_results(results, top_n)
                
                # Offer to save summary
                save = input("\nSave summary results to CSV? (y/n): ").strip().lower()
                if save == 'y':
                    filename = input("Filename (default: rolling_trend_analysis.csv): ").strip()
                    if not filename:
                        filename = "rolling_trend_analysis.csv"
                    fetcher.save_rolling_results(results, filename)
                
                # Offer to save detailed window-by-window analysis
                save_detailed = input("\nSave detailed window-by-window analysis? (y/n): ").strip().lower()
                if save_detailed == 'y':
                    filename = input("Filename (default: window_by_window_trends.csv): ").strip()
                    if not filename:
                        filename = "window_by_window_trends.csv"
                    
                    top_n_input = input("Top N markets per window (default: 10): ").strip()
                    top_n_per_window = int(top_n_input) if top_n_input else 10
                    
                    fetcher.save_window_by_window_analysis(results, filename, top_n_per_window)
        
    else:
        print("❌ Invalid choice")


if __name__ == "__main__":
    main()
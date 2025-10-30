"""
Market Analyzer
Analyzes trading patterns, volume velocity, and detects potential news events
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import statistics

logger = logging.getLogger(__name__)


class MarketAnalyzer:
    """Analyzes market data to detect patterns and news events"""
    
    def __init__(self, database, kalshi_client=None):
        """
        Initialize market analyzer
        
        Args:
            database: Database instance for accessing historical data
            kalshi_client: Optional Kalshi client for real-time data
        """
        self.db = database
        self.kalshi_client = kalshi_client
    
    async def calculate_volume_velocity(self, ticker: str, 
                                       window_minutes: int = 60) -> Dict:
        """
        Calculate volume velocity and detect news events
        
        Args:
            ticker: Market ticker
            window_minutes: Time window for analysis
        
        Returns:
            Dictionary with velocity metrics and news detection
        """
        try:
            # Get recent trades
            trades = await self.db.get_cached_trades(ticker, limit=500)
            
            if not trades:
                return {
                    "ticker": ticker,
                    "error": "No trade history available",
                    "velocity": 0,
                    "news_detected": False
                }
            
            # Parse timestamps and volumes
            now = datetime.utcnow()
            cutoff = now - timedelta(minutes=window_minutes)
            
            recent_trades = []
            for trade in trades:
                try:
                    timestamp = datetime.fromisoformat(trade['timestamp'].replace('Z', '+00:00'))
                    if timestamp >= cutoff:
                        recent_trades.append({
                            'timestamp': timestamp,
                            'volume': trade.get('count', 0)
                        })
                except:
                    continue
            
            if not recent_trades:
                return {
                    "ticker": ticker,
                    "velocity": 0,
                    "recent_volume": 0,
                    "news_detected": False,
                    "message": "No recent trades in time window"
                }
            
            # Calculate volume metrics
            recent_volume = sum(t['volume'] for t in recent_trades)
            trade_count = len(recent_trades)
            
            # Calculate velocity (volume per minute)
            velocity = recent_volume / window_minutes if window_minutes > 0 else 0
            
            # Detect potential news events using statistical analysis
            news_detected = False
            z_score = 0
            
            # Get historical baseline (last 7 days of trades)
            all_trades = trades[:min(len(trades), 1000)]
            
            if len(all_trades) >= 30:  # Need enough data for meaningful stats
                # Calculate historical hourly volumes
                hourly_volumes = self._calculate_hourly_volumes(all_trades)
                
                if len(hourly_volumes) >= 10:
                    mean_volume = statistics.mean(hourly_volumes)
                    stdev_volume = statistics.stdev(hourly_volumes) if len(hourly_volumes) > 1 else 0
                    
                    # Calculate z-score for current volume
                    if stdev_volume > 0:
                        z_score = (recent_volume - mean_volume) / stdev_volume
                        
                        # News event if volume is > 2 standard deviations above mean
                        news_detected = z_score > 2.0
            
            # Identify potential news event timestamp
            news_timestamp = None
            if news_detected and recent_trades:
                # Find the trade with highest volume spike
                max_volume_trade = max(recent_trades, key=lambda t: t['volume'])
                news_timestamp = max_volume_trade['timestamp'].isoformat()
            
            # Calculate price volatility
            price_volatility = self._calculate_price_volatility(all_trades[:100])
            
            # Trading pattern analysis
            pattern = self._analyze_trading_pattern(recent_trades)
            
            return {
                "ticker": ticker,
                "velocity": round(velocity, 2),
                "recent_volume": recent_volume,
                "trade_count": trade_count,
                "window_minutes": window_minutes,
                "news_detected": news_detected,
                "z_score": round(z_score, 2),
                "news_timestamp": news_timestamp,
                "price_volatility": round(price_volatility, 4),
                "pattern": pattern,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error calculating velocity for {ticker}: {e}")
            return {
                "ticker": ticker,
                "error": str(e),
                "velocity": 0,
                "news_detected": False
            }
    
    def _calculate_hourly_volumes(self, trades: List[Dict]) -> List[float]:
        """Calculate hourly trading volumes from trade history"""
        if not trades:
            return []
        
        # Group trades by hour
        hourly_buckets = {}
        
        for trade in trades:
            try:
                timestamp = datetime.fromisoformat(trade['timestamp'].replace('Z', '+00:00'))
                hour_key = timestamp.replace(minute=0, second=0, microsecond=0)
                
                if hour_key not in hourly_buckets:
                    hourly_buckets[hour_key] = 0
                
                hourly_buckets[hour_key] += trade.get('count', 0)
            except:
                continue
        
        return list(hourly_buckets.values())
    
    def _calculate_price_volatility(self, trades: List[Dict]) -> float:
        """Calculate price volatility from recent trades"""
        if not trades or len(trades) < 2:
            return 0.0
        
        try:
            prices = [trade.get('yes_price', 0) for trade in trades if trade.get('yes_price')]
            
            if len(prices) < 2:
                return 0.0
            
            # Calculate standard deviation of prices
            return statistics.stdev(prices)
        except:
            return 0.0
    
    def _analyze_trading_pattern(self, trades: List[Dict]) -> str:
        """
        Analyze trading pattern
        
        Returns:
            Pattern description: 'steady', 'accelerating', 'decelerating', 'spike'
        """
        if len(trades) < 10:
            return 'insufficient_data'
        
        try:
            # Split into two halves
            mid = len(trades) // 2
            first_half = trades[:mid]
            second_half = trades[mid:]
            
            first_volume = sum(t['volume'] for t in first_half)
            second_volume = sum(t['volume'] for t in second_half)
            
            # Calculate rate of change
            if first_volume == 0:
                return 'spike'
            
            change_rate = (second_volume - first_volume) / first_volume
            
            if abs(change_rate) < 0.2:
                return 'steady'
            elif change_rate > 0.5:
                return 'accelerating'
            elif change_rate < -0.5:
                return 'decelerating'
            else:
                return 'moderate_change'
                
        except:
            return 'unknown'
    
    async def detect_correlation(self, ticker1: str, ticker2: str, 
                                 window_hours: int = 24) -> Dict:
        """
        Detect price correlation between two markets
        
        Args:
            ticker1: First market ticker
            ticker2: Second market ticker
            window_hours: Time window for correlation analysis
        
        Returns:
            Correlation metrics
        """
        try:
            # Get trades for both markets
            trades1 = await self.db.get_cached_trades(ticker1, limit=200)
            trades2 = await self.db.get_cached_trades(ticker2, limit=200)
            
            if not trades1 or not trades2:
                return {
                    "error": "Insufficient trade data",
                    "correlation": 0
                }
            
            # Extract prices
            prices1 = [t.get('yes_price', 0) for t in trades1]
            prices2 = [t.get('yes_price', 0) for t in trades2]
            
            # Calculate correlation (simple version)
            min_len = min(len(prices1), len(prices2))
            if min_len < 10:
                return {
                    "error": "Insufficient data points",
                    "correlation": 0
                }
            
            prices1 = prices1[:min_len]
            prices2 = prices2[:min_len]
            
            # Calculate Pearson correlation coefficient
            correlation = self._calculate_correlation(prices1, prices2)
            
            return {
                "ticker1": ticker1,
                "ticker2": ticker2,
                "correlation": round(correlation, 3),
                "data_points": min_len,
                "interpretation": self._interpret_correlation(correlation)
            }
            
        except Exception as e:
            logger.error(f"Error detecting correlation: {e}")
            return {
                "error": str(e),
                "correlation": 0
            }
    
    def _calculate_correlation(self, x: List[float], y: List[float]) -> float:
        """Calculate Pearson correlation coefficient"""
        try:
            n = len(x)
            if n == 0:
                return 0.0
            
            mean_x = sum(x) / n
            mean_y = sum(y) / n
            
            numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
            
            sum_sq_x = sum((xi - mean_x) ** 2 for xi in x)
            sum_sq_y = sum((yi - mean_y) ** 2 for yi in y)
            
            denominator = (sum_sq_x * sum_sq_y) ** 0.5
            
            if denominator == 0:
                return 0.0
            
            return numerator / denominator
        except:
            return 0.0
    
    def _interpret_correlation(self, correlation: float) -> str:
        """Interpret correlation coefficient"""
        abs_corr = abs(correlation)
        
        if abs_corr > 0.8:
            return "very_strong"
        elif abs_corr > 0.6:
            return "strong"
        elif abs_corr > 0.4:
            return "moderate"
        elif abs_corr > 0.2:
            return "weak"
        else:
            return "very_weak"
    
    async def calculate_liquidity_score(self, ticker: str) -> Dict:
        """
        Calculate a liquidity score for a market
        
        Args:
            ticker: Market ticker
        
        Returns:
            Liquidity metrics
        """
        try:
            # Get orderbook if available
            if not self.kalshi_client:
                return {"error": "Kalshi client not available"}
            
            orderbook = await self.kalshi_client.get_orderbook(ticker)
            
            # Calculate total liquidity
            total_liquidity = 0
            for side in ['yes', 'no']:
                for level in orderbook.get(side, []):
                    total_liquidity += level.get('size', 0)
            
            # Calculate depth (number of price levels)
            depth = len(orderbook.get('yes', [])) + len(orderbook.get('no', []))
            
            # Calculate spread
            spread = self._calculate_spread(orderbook)
            
            # Calculate liquidity score (0-100)
            # Higher liquidity and depth = higher score
            # Lower spread = higher score
            liquidity_component = min(total_liquidity / 1000, 1.0) * 50
            depth_component = min(depth / 20, 1.0) * 30
            spread_component = max(0, (1 - spread) * 20)
            
            score = liquidity_component + depth_component + spread_component
            
            return {
                "ticker": ticker,
                "liquidity_score": round(score, 1),
                "total_liquidity": total_liquidity,
                "depth": depth,
                "spread": round(spread, 4),
                "rating": self._rate_liquidity(score)
            }
            
        except Exception as e:
            logger.error(f"Error calculating liquidity score: {e}")
            return {"error": str(e)}
    
    def _calculate_spread(self, orderbook: Dict) -> float:
        """Calculate bid-ask spread"""
        try:
            yes_levels = orderbook.get('yes', [])
            
            if not yes_levels or len(yes_levels) < 2:
                return 1.0  # Maximum spread if no data
            
            # Sort by price
            asks = sorted([l for l in yes_levels], key=lambda x: x['price'])
            bids = sorted([l for l in yes_levels], key=lambda x: x['price'], reverse=True)
            
            if asks and bids:
                spread = asks[0]['price'] - bids[0]['price']
                return max(0, spread)
            
            return 1.0
        except:
            return 1.0
    
    def _rate_liquidity(self, score: float) -> str:
        """Rate liquidity based on score"""
        if score >= 80:
            return "excellent"
        elif score >= 60:
            return "good"
        elif score >= 40:
            return "moderate"
        elif score >= 20:
            return "low"
        else:
            return "very_low"
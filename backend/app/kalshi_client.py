"""
Kalshi API Client
Handles authentication and API requests to Kalshi
"""

import requests
import time
import hmac
import hashlib
import base64
from typing import Dict, List, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class KalshiClient:
    """Client for interacting with Kalshi API"""
    
    BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
    
    def __init__(self, api_key: str, private_key: str):
        """
        Initialize Kalshi client
        
        Args:
            api_key: Kalshi API key ID
            private_key: Private key PEM content (not path)
        """
        self.api_key = api_key
        self.private_key = private_key
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json"
        })
    
    def _generate_signature(self, method: str, path: str, body: str = "") -> str:
        """Generate HMAC signature for request"""
        timestamp = str(int(time.time() * 1000))
        
        # Create signature string
        sig_string = f"{timestamp}{method}{path}{body}"
        
        # Sign with private key
        signature = hmac.new(
            self.private_key.encode(),
            sig_string.encode(),
            hashlib.sha256
        ).hexdigest()
        
        return f"KALSHI {self.api_key}:{timestamp}:{signature}"
    
    async def _request(self, method: str, endpoint: str, params: Optional[Dict] = None, 
                      json_data: Optional[Dict] = None) -> Dict:
        """Make authenticated request to Kalshi API"""
        url = f"{self.BASE_URL}{endpoint}"
        
        # Generate signature
        body = ""
        if json_data:
            import json
            body = json.dumps(json_data)
        
        signature = self._generate_signature(method, endpoint, body)
        
        headers = {
            "Authorization": signature
        }
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_data,
                timeout=30
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
    
    async def get_markets(self, status: str = "open", limit: int = 100) -> List[Dict]:
        """
        Get list of markets
        
        Args:
            status: Market status filter (open, closed, settled)
            limit: Maximum number of markets to return
        
        Returns:
            List of market dictionaries
        """
        try:
            params = {
                "status": status,
                "limit": limit
            }
            
            response = await self._request("GET", "/markets", params=params)
            return response.get("markets", [])
            
        except Exception as e:
            logger.error(f"Error fetching markets: {e}")
            return []
    
    async def get_market(self, ticker: str) -> Dict:
        """
        Get detailed information about a specific market
        
        Args:
            ticker: Market ticker symbol
        
        Returns:
            Market dictionary
        """
        try:
            response = await self._request("GET", f"/markets/{ticker}")
            return response.get("market", {})
            
        except Exception as e:
            logger.error(f"Error fetching market {ticker}: {e}")
            raise
    
    async def get_orderbook(self, ticker: str, depth: int = 10) -> Dict:
        """
        Get orderbook for a specific market
        
        Args:
            ticker: Market ticker symbol
            depth: Number of price levels to return
        
        Returns:
            Orderbook dictionary with 'yes' and 'no' sides
        """
        try:
            params = {"depth": depth}
            response = await self._request("GET", f"/markets/{ticker}/orderbook", params=params)
            
            orderbook = response.get("orderbook", {})
            
            # Format orderbook
            formatted = {
                "yes": [],
                "no": []
            }
            
            # Parse yes side
            for level in orderbook.get("yes", []):
                formatted["yes"].append({
                    "price": level.get("price", 0),
                    "size": level.get("size", 0)
                })
            
            # Parse no side
            for level in orderbook.get("no", []):
                formatted["no"].append({
                    "price": level.get("price", 0),
                    "size": level.get("size", 0)
                })
            
            return formatted
            
        except Exception as e:
            logger.error(f"Error fetching orderbook for {ticker}: {e}")
            raise
    
    async def get_trades(self, ticker: str, limit: int = 100) -> List[Dict]:
        """
        Get recent trades for a specific market
        
        Args:
            ticker: Market ticker symbol
            limit: Maximum number of trades to return
        
        Returns:
            List of trade dictionaries
        """
        try:
            params = {"limit": limit}
            response = await self._request("GET", f"/markets/{ticker}/trades", params=params)
            
            trades = response.get("trades", [])
            
            # Format trades
            formatted_trades = []
            for trade in trades:
                formatted_trades.append({
                    "trade_id": trade.get("trade_id", ""),
                    "ticker": ticker,
                    "price": trade.get("yes_price", 0),
                    "count": trade.get("count", 0),
                    "yes_price": trade.get("yes_price", 0),
                    "no_price": trade.get("no_price", 0),
                    "taker_side": trade.get("taker_side", ""),
                    "timestamp": trade.get("created_time", ""),
                    "created_time": trade.get("created_time", "")
                })
            
            return formatted_trades
            
        except Exception as e:
            logger.error(f"Error fetching trades for {ticker}: {e}")
            return []
    
    async def get_market_history(self, ticker: str, start_time: Optional[str] = None,
                                 end_time: Optional[str] = None) -> List[Dict]:
        """
        Get historical price data for a market
        
        Args:
            ticker: Market ticker symbol
            start_time: Start time in ISO format
            end_time: End time in ISO format
        
        Returns:
            List of historical price points
        """
        try:
            params = {}
            if start_time:
                params["start_time"] = start_time
            if end_time:
                params["end_time"] = end_time
            
            response = await self._request("GET", f"/markets/{ticker}/history", params=params)
            return response.get("history", [])
            
        except Exception as e:
            logger.error(f"Error fetching history for {ticker}: {e}")
            return []
    
    async def search_markets(self, query: str, limit: int = 20) -> List[Dict]:
        """
        Search markets by title or ticker
        
        Args:
            query: Search query
            limit: Maximum number of results
        
        Returns:
            List of matching markets
        """
        try:
            params = {
                "query": query,
                "limit": limit
            }
            
            response = await self._request("GET", "/markets/search", params=params)
            return response.get("markets", [])
            
        except Exception as e:
            logger.error(f"Error searching markets: {e}")
            return []
    
    def close(self):
        """Close the session"""
        self.session.close()
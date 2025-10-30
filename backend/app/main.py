from fastapi import FastAPI, HTTPException, WebSocket, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import asyncio
import logging
from typing import Optional, List
from datetime import datetime
import os

# Import your modules
from kalshi_client import KalshiClient
from market_analyzer import MarketAnalyzer
from database import Database

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Kalshi Market Tools API",
    description="Advanced analytics for event contract trading",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances
kalshi_client = None
market_analyzer = None
db = None
markets_cache = {}
cache_timestamp = None

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global kalshi_client, market_analyzer, db
    
    try:
        # Initialize database with PostgreSQL URL
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            logger.warning("DATABASE_URL not configured - running without database")
            db = None
        else:
            db = Database(database_url)
            await db.initialize()
        
        # Initialize Kalshi client
        api_key = os.getenv("KALSHI_API_KEY")
        private_key = os.getenv("KALSHI_PRIVATE_KEY")
        
        if api_key and private_key:
            kalshi_client = KalshiClient(api_key, private_key)
            logger.info("Kalshi client initialized")
        else:
            logger.warning("Kalshi credentials not found, running in mock mode")
        
        # Initialize analyzer
        market_analyzer = MarketAnalyzer(db)
        
        # Start background tasks
        asyncio.create_task(update_markets_cache())
        
    except Exception as e:
        logger.error(f"Startup error: {e}")

async def update_markets_cache():
    """Background task to update markets cache"""
    global markets_cache, cache_timestamp
    
    while True:
        try:
            if kalshi_client:
                markets = await kalshi_client.get_all_markets(status="open", max_markets=100)
                markets_cache = {m['ticker']: m for m in markets}
                cache_timestamp = datetime.now()
                logger.info(f"Updated cache with {len(markets)} markets")
        except Exception as e:
            logger.error(f"Cache update error: {e}")
        
        await asyncio.sleep(30)  # Update every 30 seconds

@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Kalshi Market Tools API",
        "kalshi_connected": kalshi_client is not None,
        "cache_size": len(markets_cache),
        "last_cache_update": cache_timestamp.isoformat() if cache_timestamp else None
    }

@app.get("/api/markets")
async def get_markets(
    search: Optional[str] = None,
    category: Optional[str] = None,
    min_volume: Optional[int] = None,
    max_volume: Optional[int] = None,
    limit: int = Query(default=50, le=200)
):
    """Get filtered list of markets"""
    try:
        # Use cache if available
        if markets_cache:
            markets = list(markets_cache.values())
        elif kalshi_client:
            markets = await kalshi_client.get_markets(limit=limit, status="open")
        else:
            # Return mock data in development
            return {"markets": [], "total": 0, "mock_mode": True}
        
        # Apply filters
        if search:
            markets = [m for m in markets if search.lower() in m['title'].lower() or search.lower() in m['ticker'].lower()]
        if category:
            markets = [m for m in markets if m.get('category') == category]
        if min_volume:
            markets = [m for m in markets if m.get('volume', 0) >= min_volume]
        if max_volume:
            markets = [m for m in markets if m.get('volume', 0) <= max_volume]
        
        # Add volatility scores
        for market in markets[:limit]:
            market['volatility'] = await market_analyzer.calculate_volatility(market['ticker'])
        
        return {
            "markets": markets[:limit],
            "total": len(markets)
        }
        
    except Exception as e:
        logger.error(f"Error fetching markets: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/markets/{ticker}/impact")
async def calculate_impact(
    ticker: str,
    side: str = Query(..., regex="^(yes|no)$"),
    quantity: int = Query(..., ge=1, le=10000)
):
    """Calculate market impact for an order"""
    try:
        # Get orderbook
        if kalshi_client:
            orderbook = await kalshi_client.get_orderbook(ticker, depth=20)
        else:
            # Mock data for testing
            orderbook = {"yes": [[52, 200], [53, 300], [54, 400]], "no": [[48, 200], [47, 300], [46, 400]]}
        
        # Calculate impact
        impact = market_analyzer.calculate_order_impact(orderbook, side, quantity)
        
        return impact
        
    except Exception as e:
        logger.error(f"Error calculating impact: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/markets/{ticker}/velocity")
async def analyze_velocity(ticker: str):
    """Analyze volume velocity for a market"""
    try:
        # Get trades
        if kalshi_client:
            trades = await kalshi_client.get_trades(ticker, limit=500)
        else:
            # Mock data
            trades = []
        
        # Analyze velocity
        analysis = await market_analyzer.analyze_volume_velocity(trades)
        
        # Detect news events (simplified for now)
        news_events = await detect_news_events(trades)
        analysis['news_events'] = news_events
        
        return analysis
        
    except Exception as e:
        logger.error(f"Error analyzing velocity: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def detect_news_events(trades):
    """Detect potential news events from trading patterns"""
    # Simplified news detection - you can enhance this with actual news API
    events = []
    
    # Look for volume spikes
    if len(trades) > 20:
        avg_volume = sum(t.get('count', 0) for t in trades) / len(trades)
        for i, trade in enumerate(trades):
            if trade.get('count', 0) > avg_volume * 2:
                events.append({
                    "timestamp": trade.get('timestamp'),
                    "description": "Significant volume spike detected",
                    "volume_spike": trade.get('count', 0),
                    "price_at_event": trade.get('price', 0)
                })
                if len(events) >= 3:
                    break
    
    return events

@app.websocket("/ws/markets")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await websocket.accept()
    subscribed_tickers = set()
    
    try:
        while True:
            # Receive messages
            data = await websocket.receive_json()
            
            if data.get("action") == "subscribe":
                ticker = data.get("ticker")
                if ticker:
                    subscribed_tickers.add(ticker)
                    await websocket.send_json({"type": "subscribed", "ticker": ticker})
            
            elif data.get("action") == "unsubscribe":
                ticker = data.get("ticker")
                if ticker in subscribed_tickers:
                    subscribed_tickers.remove(ticker)
                    await websocket.send_json({"type": "unsubscribed", "ticker": ticker})
            
            # Send updates for subscribed tickers
            for ticker in subscribed_tickers:
                if kalshi_client:
                    orderbook = await kalshi_client.get_orderbook(ticker)
                    await websocket.send_json({
                        "type": "orderbook_update",
                        "ticker": ticker,
                        "data": orderbook
                    })
            
            await asyncio.sleep(2)  # Update every 2 seconds
            
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        await websocket.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
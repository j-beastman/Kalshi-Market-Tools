import os
import json
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

# Import your existing modules
from .kalshi_client import KalshiClient
from .database import Database
from .market_analyzer import MarketAnalyzer

# ---------- Config ----------
KALSHI_API_KEY = os.getenv("KALSHI_API_KEY")
KALSHI_PRIVATE_KEY = os.getenv("KALSHI_PRIVATE_KEY")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/kalshi_dev")
PULL_INTERVAL_SECONDS = int(os.getenv("PULL_INTERVAL_SECONDS", "30"))
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

# ---------- App Setup ----------
app = FastAPI(title="Kalshi Pipeline API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ---------- Global instances ----------
db = None
kalshi_client = None
_subscribers: List[asyncio.Queue] = []

# ---------- SSE Broadcasting ----------
async def broadcast(payload: Dict[str, Any]):
    for q in list(_subscribers):
        try:
            await q.put(payload)
        except:
            pass

async def sse_generator():
    queue: asyncio.Queue = asyncio.Queue()
    _subscribers.append(queue)
    try:
        while True:
            item = await queue.get()
            yield f"data: {json.dumps(item)}\n\n"
    finally:
        if queue in _subscribers:
            _subscribers.remove(queue)

@app.get("/stream")
async def stream():
    return StreamingResponse(sse_generator(), media_type="text/event-stream")

# ---------- Data Fetching ----------
async def fetch_and_store_markets():
    """Fetch markets from Kalshi and store in PostgreSQL"""
    try:
        if not kalshi_client:
            print("⚠️ No Kalshi client - using mock data")
            return await fetch_mock_data()
        
        # Get open markets
        markets = await kalshi_client.get_markets(status="open", limit=50)
        
        # Filter out complex multi-game markets that might not have standard orderbooks
        filtered_markets = [
            m for m in markets 
            if not m.get("ticker", "").startswith("KXMVENFLMULTIGAME")
        ]
        
        print(f"📊 Processing {len(filtered_markets)} markets (filtered from {len(markets)})")
        
        # Process each market
        market_updates = []
        for market in filtered_markets:
            ticker = market.get("ticker")
            
            try:
                # Get orderbook for pricing
                orderbook = await kalshi_client.get_orderbook(ticker, depth=5)
                
                # Calculate prices based on orderbook
                yes_price = 50  # Default
                no_price = 50
                yes_bid = 0
                yes_ask = 100
                no_bid = 0
                no_ask = 100
                
                # Extract best bid/ask from yes side
                if orderbook.get("yes") and len(orderbook["yes"]) > 0:
                    yes_levels = sorted(orderbook["yes"], key=lambda x: x.get("price", 0))
                    if yes_levels:
                        yes_bid = yes_levels[0].get("price", 0)
                        yes_ask = yes_levels[-1].get("price", 100)
                        yes_price = (yes_bid + yes_ask) / 2
                
                # Extract best bid/ask from no side
                if orderbook.get("no") and len(orderbook["no"]) > 0:
                    no_levels = sorted(orderbook["no"], key=lambda x: x.get("price", 0))
                    if no_levels:
                        no_bid = no_levels[0].get("price", 0)
                        no_ask = no_levels[-1].get("price", 100)
                        no_price = (no_bid + no_ask) / 2
                
                # If we don't have orderbook data, try to use market's last price
                if yes_price == 50 and no_price == 50:
                    yes_price = market.get("yes_price", market.get("last_price", 50))
                    no_price = market.get("no_price", 100 - yes_price)
                
                # Update market data
                market.update({
                    "yes_price": yes_price,
                    "no_price": no_price,
                    "yes_bid": yes_bid,
                    "yes_ask": yes_ask,
                    "no_bid": no_bid,
                    "no_ask": no_ask
                })
                
                # Cache in database
                await db.cache_market(market)
                
                # Get recent trades (with error handling)
                try:
                    trades = await kalshi_client.get_trades(ticker, limit=20)
                    for trade in trades:
                        await db.cache_trade(ticker, trade)
                except Exception as e:
                    print(f"Could not fetch trades for {ticker}: {e}")
                
                # Prepare update for SSE
                market_updates.append({
                    "ticker": ticker,
                    "title": market.get("title"),
                    "category": market.get("category"),
                    "yes_price": round(yes_price),
                    "no_price": round(no_price),
                    "volume_24h": market.get("volume", 0)
                })
                
                print(f"✅ Processed {ticker}: YES={round(yes_price)}¢ NO={round(no_price)}¢")
                
            except Exception as e:
                print(f"⚠️ Error processing {ticker}: {e}")
                # Still add the market with basic info
                market_updates.append({
                    "ticker": ticker,
                    "title": market.get("title"),
                    "category": market.get("category"),
                    "yes_price": market.get("yes_price", 50),
                    "no_price": market.get("no_price", 50),
                    "volume_24h": market.get("volume", 0)
                })
        
        # Broadcast updates
        if market_updates:
            await broadcast({
                "type": "upsert",
                "markets": market_updates,
                "ts": datetime.now(timezone.utc).isoformat()
            })
            
        print(f"✅ Updated {len(market_updates)} markets")
        
    except Exception as e:
        print(f"❌ Error fetching markets: {e}")
        import traceback
        traceback.print_exc()
        return await fetch_mock_data()

async def fetch_mock_data():
    """Fallback mock data when Kalshi unavailable"""
    import random
    mock_markets = [
        {"ticker": "RATE-CUT-DEC", "title": "Fed cuts rates in December?", 
         "category": "Fed", "yes_price": random.randint(30,70), 
         "no_price": 100-random.randint(30,70), "volume_24h": random.randint(1000,5000)}
    ]
    await broadcast({"type": "upsert", "markets": mock_markets, 
                    "ts": datetime.now(timezone.utc).isoformat()})

# ---------- Routes ----------
@app.get("/health")
async def health():
    return {
        "ok": True,
        "time": datetime.now(timezone.utc).isoformat(),
        "database": db is not None,
        "kalshi": kalshi_client is not None
    }

@app.get("/markets")
async def get_markets():
    """Get all cached markets from database"""
    if not db:
        return []
    
    markets = await db.get_cached_markets(status="open", limit=100)
    
    # Format for frontend
    formatted = []
    for market in markets:
        formatted.append({
            "ticker": market.get("ticker"),
            "title": market.get("title"),
            "category": market.get("category"),
            "yes_price": market.get("yes_price", 0),
            "no_price": market.get("no_price", 0),
            "yes_bid": market.get("yes_bid", 0),
            "yes_ask": market.get("yes_ask", 0),
            "no_bid": market.get("no_bid", 0),
            "no_ask": market.get("no_ask", 0),
            "volume_24h": market.get("volume", 0),
            "last_update": market.get("last_updated")
        })
    
    return formatted

@app.get("/markets/{ticker}")
async def get_market_detail(ticker: str):
    """Get detailed market data including historical trades"""
    if not db:
        return JSONResponse({"error": "Database not available"}, status_code=503)
    
    market = await db.get_market_by_ticker(ticker)
    if not market:
        return JSONResponse({"error": "Market not found"}, status_code=404)
    
    trades = await db.get_cached_trades(ticker, limit=500)
    
    # Calculate volume velocity if analyzer available
    velocity = None
    if db and kalshi_client:
        analyzer = MarketAnalyzer(db, kalshi_client)
        velocity = await analyzer.calculate_volume_velocity(ticker)
    
    return {
        "ticker": ticker,
        "title": market.get("title"),
        "category": market.get("category"),
        "series": trades,
        "velocity": velocity
    }

# ---------- Background Tasks ----------
async def pull_loop():
    """Main data fetching loop"""
    while True:
        await fetch_and_store_markets()
        await asyncio.sleep(PULL_INTERVAL_SECONDS)

async def cleanup_loop():
    """Cleanup old data periodically"""
    while True:
        if db:
            await db.clear_old_data(days=7)
        await asyncio.sleep(3600 * 24)  # Run daily

# ---------- Startup/Shutdown ----------
@app.on_event("startup")
async def startup():
    global db, kalshi_client
    
    # Initialize database
    if DATABASE_URL:
        try:
            db = Database(DATABASE_URL)
            await db.initialize()
            print("✅ Database connected")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
    
    # Initialize Kalshi client
    if KALSHI_API_KEY and KALSHI_PRIVATE_KEY:
        try:
            kalshi_client = KalshiClient(KALSHI_API_KEY, KALSHI_PRIVATE_KEY)
            print("✅ Kalshi client initialized")
        except Exception as e:
            print(f"❌ Kalshi client failed: {e}")
    else:
        print("⚠️ No Kalshi credentials - using mock mode")
    
    # Start background tasks
    asyncio.create_task(pull_loop())
    asyncio.create_task(cleanup_loop())
    
    # Initial data fetch
    await fetch_and_store_markets()

@app.on_event("shutdown")
async def shutdown():
    if db:
        await db.close()
    if kalshi_client:
        kalshi_client.close()

# --------- Testing ----------
@app.get("/debug/kalshi-test")
async def debug_kalshi():
    """Test Kalshi API directly"""
    if not kalshi_client:
        return {"error": "No Kalshi client configured"}
    
    try:
        # Get first few markets
        markets = await kalshi_client.get_markets(status="open", limit=5)
        
        if not markets:
            return {"error": "No markets returned"}
        
        # Test orderbook for first market
        ticker = markets[0].get("ticker")
        
        # Get raw response
        response = await kalshi_client._request("GET", f"/markets/{ticker}/orderbook", params={"depth": 5})
        
        return {
            "ticker": ticker,
            "raw_orderbook_response": response,
            "market_count": len(markets),
            "sample_market": markets[0]
        }
        
    except Exception as e:
        import traceback
        return {
            "error": str(e),
            "traceback": traceback.format_exc()
        }
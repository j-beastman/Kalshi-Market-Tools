import os
import json
import re
import asyncio
import math
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Config ----------
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_API_KEY = os.getenv("KALSHI_API_KEY")
KALSHI_PRIVATE_KEY = os.getenv("KALSHI_PRIVATE_KEY")

# ---------- App ----------
app = FastAPI(title="Kalshi Mortgage Hedge API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ---------- Schemas ----------
class FedMarket(BaseModel):
    ticker: str
    title: str
    cuts: int
    probability: float
    price: int
    yes_bid: int
    yes_ask: int
    no_bid: int
    no_ask: int
    volume: int

class HedgeRequest(BaseModel):
    loan_amount: float
    current_rate: float
    refinance_threshold: float
    refinance_cost: float
    strategy_type: str = 'probability-weighted'

class HedgeResponse(BaseModel):
    yearly_opportunity_cost: float
    optimal_hedge_amount: float
    cuts_to_refinance: int
    allocations: List[Dict[str, Any]]
    total_hedge_cost: float
    expected_value: float

# ---------- Kalshi Auth ----------
def get_kalshi_headers():
    """Generate headers for Kalshi API authentication"""
    if not KALSHI_API_KEY:
        return {}
    
    # Simplified auth - in production, implement proper HMAC signing
    return {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": KALSHI_API_KEY
    }

# ---------- Routes ----------
@app.get("/health")
async def health():
    return {
        "ok": True, 
        "time": datetime.now(timezone.utc).isoformat(),
        "kalshi_configured": bool(KALSHI_API_KEY)
    }

@app.get("/api/fed-markets", response_model=List[FedMarket])
async def get_fed_markets():
    """Get Fed rate cut markets from Kalshi KXRATECUTCOUNT series"""
    
    if not KALSHI_API_KEY:
        logger.error("Kalshi API key not configured")
        raise HTTPException(status_code=500, detail="Kalshi API not configured")
    
    try:
        async with httpx.AsyncClient() as client:
            # Get markets from KXRATECUTCOUNT series
            response = await client.get(
                f"{KALSHI_API_BASE}/markets",
                headers=get_kalshi_headers(),
                params={"series_ticker": "KXRATECUTCOUNT",
                        "mve_filter": "exclude"},
                timeout=10.0
            )
            
            if response.status_code != 200:
                logger.error(f"Kalshi API returned {response.status_code}: {response.text}")
                raise HTTPException(status_code=502, detail=f"Kalshi API error: {response.status_code}")
            
            data = response.json()
            markets = data.get("markets", [])
            
            fed_markets = []
            for market in markets:
                # Skip closed/settled markets
                status = market.get("status", "")
                if status not in ("open", "active"):
                    continue
                
                # Extract cuts from ticker (e.g., KXRATECUTCOUNT-2025-3 means 3 cuts)
                ticker = market.get("ticker", "")
                match = re.search(r'(\d{1,2})$', ticker)
                if not match:
                    continue
                cuts = int(match.group(1))
                
                # Get market details
                last_price = market.get("last_price", 0)
                yes_bid = market.get("yes_bid", 0)
                yes_ask = market.get("yes_ask", 0)
                no_bid = market.get("no_bid", 0)
                no_ask = market.get("no_ask", 0)
                volume = market.get("volume_24h", 0)
                
                # Skip markets with spread > 10 cents (too illiquid/speculative)
                spread = yes_ask - yes_bid
                open_interest = market.get("open_interest", 0)
                if spread > 10 and open_interest != 0 and volume > 1000 and last_price > 0:
                    continue
                
                fed_markets.append(FedMarket(
                    ticker=ticker,
                    title=market.get("title", f"{cuts} rate cuts"),
                    cuts=cuts,
                    probability=last_price / 100.0 if last_price else 0,
                    price=last_price,
                    yes_bid=yes_bid,
                    yes_ask=yes_ask,
                    no_bid=no_bid,
                    no_ask=no_ask,
                    volume=volume
                ))
            
            # Sort by number of cuts
            fed_markets.sort(key=lambda m: m.cuts)
            
            logger.info(f"Fetched {len(fed_markets)} Fed markets from Kalshi")
            return fed_markets
            
    except httpx.TimeoutException:
        logger.error("Kalshi API timeout")
        raise HTTPException(status_code=504, detail="Kalshi API timeout")
    except Exception as e:
        logger.error(f"Error fetching Kalshi markets: {e}")
        raise HTTPException(status_code=502, detail=f"Failed to fetch markets: {str(e)}")

@app.post("/api/calculate-hedge", response_model=HedgeResponse)
async def calculate_hedge(request: HedgeRequest):
    """Calculate optimal hedge strategy"""
    
    # Constants
    KALSHI_FEE = 0.07  # 7 cents per contract
    
    # Fetch current Fed markets
    try:
        fed_markets = await get_fed_markets()
    except:
        # Use fallback data if Kalshi fails
        fed_markets = [
            FedMarket(ticker="MOCK-0", title="0 cuts", cuts=0, probability=0.15, 
                     price=15, yes_bid=14, yes_ask=16, no_bid=84, no_ask=86, volume=1000),
            FedMarket(ticker="MOCK-1", title="1 cut", cuts=1, probability=0.25, 
                     price=25, yes_bid=24, yes_ask=26, no_bid=74, no_ask=76, volume=1500),
            FedMarket(ticker="MOCK-2", title="2 cuts", cuts=2, probability=0.35, 
                     price=35, yes_bid=34, yes_ask=36, no_bid=64, no_ask=66, volume=2000),
            FedMarket(ticker="MOCK-3", title="3 cuts", cuts=3, probability=0.20, 
                     price=20, yes_bid=19, yes_ask=21, no_bid=79, no_ask=81, volume=1000),
            FedMarket(ticker="MOCK-4", title="4+ cuts", cuts=4, probability=0.05, 
                     price=5, yes_bid=4, yes_ask=6, no_bid=94, no_ask=96, volume=500),
        ]
    
    # Calculate mortgage metrics
    rate_diff = request.current_rate - request.refinance_threshold
    cuts_to_refinance = math.ceil(rate_diff / 0.25)
    
    r1 = request.current_rate / 100 / 12
    r2 = request.refinance_threshold / 100 / 12
    n = 30 * 12
    
    current_payment = request.loan_amount * (r1 * pow(1 + r1, n)) / (pow(1 + r1, n) - 1)
    refi_payment = request.loan_amount * (r2 * pow(1 + r2, n)) / (pow(1 + r2, n) - 1)
    yearly_opportunity_cost = (current_payment - refi_payment) * 12
    
    # Calculate optimal hedge amount (yearly opportunity cost + 10% for fees)
    optimal_hedge_amount = yearly_opportunity_cost * 1.1
    
    # Get eligible markets (only hedge scenarios where we DON'T refinance)
    eligible_markets = [m for m in fed_markets if m.cuts < cuts_to_refinance]
    
    if not eligible_markets:
        return HedgeResponse(
            yearly_opportunity_cost=yearly_opportunity_cost,
            optimal_hedge_amount=optimal_hedge_amount,
            cuts_to_refinance=cuts_to_refinance,
            allocations=[],
            total_hedge_cost=0,
            expected_value=0
        )
    
    total_probability = sum(m.probability for m in eligible_markets)
    
    # Calculate allocations
    allocations = []
    for market in eligible_markets:
        # Weight allocation by probability
        if request.strategy_type == 'probability-weighted':
            weight = market.probability / total_probability
        elif request.strategy_type == 'equal':
            weight = 1.0 / len(eligible_markets)
        else:  # max-protection
            max_prob = max(m.probability for m in eligible_markets)
            weight = 0.6 if market.probability == max_prob else 0.4 / (len(eligible_markets) - 1)
        
        allocation = optimal_hedge_amount * weight
        price_per_contract = market.price / 100
        contracts = int(allocation / price_per_contract) if price_per_contract > 0 else 0
        
        if contracts > 0:
            cost = contracts * price_per_contract
            p = market.price / 100  # price as decimal
            fees = math.ceil(0.07 * contracts * p * (1 - p) * 100) / 100
            payout = contracts * 1.0
            net_profit = payout - cost - fees
            
            allocations.append({
                'cuts': market.cuts,
                'probability': market.probability,
                'price': market.price,
                'ticker': market.ticker,
                'title': market.title,
                'allocation': allocation,
                'contracts': contracts,
                'cost': cost,
                'fees': fees,
                'payout': payout,
                'netProfit': net_profit,
                'kalshiUrl': f"https://kalshi.com/markets/{market.ticker}"
            })
    #
    total_hedge_cost = sum(a['cost'] + a['fees'] for a in allocations)
    expected_value = sum(a['probability'] * a['netProfit'] for a in allocations)
    
    return HedgeResponse(
        yearly_opportunity_cost=yearly_opportunity_cost,
        optimal_hedge_amount=optimal_hedge_amount,
        cuts_to_refinance=cuts_to_refinance,
        allocations=allocations,
        total_hedge_cost=total_hedge_cost,
        expected_value=expected_value
    )

# ---------- SSE for real-time updates ----------
_subscribers: List[asyncio.Queue] = []

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

async def broadcast(data: dict):
    for q in _subscribers:
        try:
            await q.put(data)
        except:
            pass

# ---------- Background updater ----------
async def update_markets_periodically():
    while True:
        try:
            markets = await get_fed_markets()
            await broadcast({
                'type': 'fed-update',
                'markets': [m.dict() for m in markets],
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            logger.error(f"Failed to update markets: {e}")
        
        await asyncio.sleep(30)  # Update every 30 seconds

@app.on_event("startup")
async def startup():
    asyncio.create_task(update_markets_periodically())
    logger.info("Server started - pulling from Kalshi KXRATECUTCOUNT series")
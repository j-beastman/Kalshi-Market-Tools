import os
import json
import asyncio
import math
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from sqlalchemy import create_engine, Column, String, Integer, Float, BigInteger, DateTime, ForeignKey, select, func
from sqlalchemy.orm import sessionmaker, DeclarativeBase, mapped_column, Mapped

# Import Kalshi client if available
try:
    from app.kalshi_client import KalshiClient
    KALSHI_AVAILABLE = True
except ImportError:
    KALSHI_AVAILABLE = False
    print("Warning: Kalshi client not available, using mock data")

# ---------- Config ----------
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./dev.db")
PULL_INTERVAL_SECONDS = int(os.getenv("PULL_INTERVAL_SECONDS", "30"))

# Kalshi API credentials (from Railway environment)
KALSHI_API_KEY = os.getenv("KALSHI_API_KEY")
KALSHI_PRIVATE_KEY = os.getenv("KALSHI_PRIVATE_KEY")

# ---------- DB setup ----------
class Base(DeclarativeBase):
    pass

class Market(Base):
    __tablename__ = "markets"
    ticker: Mapped[str] = mapped_column(String, primary_key=True)
    title: Mapped[str] = mapped_column(String, nullable=False)
    category: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    last_update: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    # Fed-specific fields
    rate_cuts: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_fed_market: Mapped[Optional[bool]] = mapped_column(Integer, nullable=True, default=False)

class Quote(Base):
    __tablename__ = "quotes"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String, ForeignKey("markets.ticker"), index=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, default=lambda: datetime.now(timezone.utc))
    yes_bid: Mapped[int] = mapped_column(Integer, default=0)
    yes_ask: Mapped[int] = mapped_column(Integer, default=0)
    no_bid: Mapped[int] = mapped_column(Integer, default=0)
    no_ask: Mapped[int] = mapped_column(Integer, default=0)
    yes_last: Mapped[int] = mapped_column(Integer, default=0)
    no_last: Mapped[int] = mapped_column(Integer, default=0)
    volume: Mapped[int] = mapped_column(Integer, default=0)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)

def init_db():
    Base.metadata.create_all(bind=engine)

# ---------- Kalshi Client Setup ----------
kalshi_client = None
if KALSHI_AVAILABLE and KALSHI_API_KEY and KALSHI_PRIVATE_KEY:
    kalshi_client = KalshiClient(KALSHI_API_KEY, KALSHI_PRIVATE_KEY)
    print("Kalshi client initialized")

# ---------- App ----------
app = FastAPI(title="Kalshi Mortgage Hedge API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ---------- SSE subscribers ----------
_subscribers: List[asyncio.Queue] = []

async def broadcast(payload: Dict[str, Any]):
    for q in list(_subscribers):
        try:
            await q.put(payload)
        except Exception:
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
    fed_markets: Optional[List[Dict[str, Any]]] = None

class HedgeResponse(BaseModel):
    yearly_opportunity_cost: float
    optimal_hedge_amount: float
    cuts_to_refinance: int
    allocations: List[Dict[str, Any]]
    total_hedge_cost: float
    expected_value: float

# ---------- Helper Functions ----------
def extract_rate_cuts(title: str, ticker: str) -> int:
    """Extract number of rate cuts from market title or ticker"""
    title_lower = title.lower()
    ticker_lower = ticker.lower()
    
    # Check for explicit numbers in title
    if "no cut" in title_lower or "zero cut" in title_lower or "0 cut" in title_lower:
        return 0
    elif "one cut" in title_lower or "1 cut" in title_lower or "exactly 1" in title_lower:
        return 1
    elif "two cut" in title_lower or "2 cut" in title_lower or "exactly 2" in title_lower:
        return 2
    elif "three cut" in title_lower or "3 cut" in title_lower or "exactly 3" in title_lower:
        return 3
    elif "four cut" in title_lower or "4 cut" in title_lower or "exactly 4" in title_lower:
        return 4
    elif "five cut" in title_lower or "5 cut" in title_lower or "exactly 5" in title_lower:
        return 5
    elif "six cut" in title_lower or "6 cut" in title_lower or "exactly 6" in title_lower:
        return 6
    
    # Check ticker patterns (e.g., FEDZ24-2 for 2 cuts)
    if "-" in ticker:
        parts = ticker.split("-")
        if len(parts) > 1 and parts[-1].isdigit():
            return int(parts[-1])
    
    # Default to 0 if uncertain
    return 0

def is_fed_rate_market(title: str, ticker: str) -> bool:
    """Determine if a market is a Fed rate cut market"""
    title_lower = title.lower()
    ticker_lower = ticker.lower()
    
    fed_keywords = ["fed", "fomc", "rate cut", "federal reserve", "basis point", "bp cut"]
    return any(keyword in title_lower for keyword in fed_keywords) or "fed" in ticker_lower

# ---------- Routes ----------
@app.get("/health")
def health():
    return {
        "ok": True, 
        "time": datetime.now(timezone.utc).isoformat(),
        "kalshi_connected": kalshi_client is not None
    }

@app.get("/api/fed-markets", response_model=List[FedMarket])
async def get_fed_markets():
    """Get all Fed rate cut markets with live data"""
    
    fed_markets = []
    
    with SessionLocal() as s:
        # Get all Fed markets from database
        markets = s.execute(
            select(Market).where(Market.is_fed_market == True)
        ).scalars().all()
        
        for market in markets:
            # Get latest quote
            latest = s.execute(
                select(Quote)
                .where(Quote.ticker == market.ticker)
                .order_by(Quote.ts.desc())
                .limit(1)
            ).scalars().first()
            
            if latest:
                cuts = market.rate_cuts or extract_rate_cuts(market.title, market.ticker)
                probability = latest.yes_last / 100.0 if latest.yes_last else 0
                
                fed_markets.append(FedMarket(
                    ticker=market.ticker,
                    title=market.title,
                    cuts=cuts,
                    probability=probability,
                    price=latest.yes_last or 0,
                    yes_bid=latest.yes_bid or 0,
                    yes_ask=latest.yes_ask or 0,
                    no_bid=latest.no_bid or 0,
                    no_ask=latest.no_ask or 0,
                    volume=latest.volume or 0
                ))
    
    # If no Fed markets in DB, return mock data
    if not fed_markets:
        fed_markets = [
            FedMarket(ticker="FEDZ24", title="No Fed cuts by Dec 2025", cuts=0, probability=0.10, 
                     price=10, yes_bid=9, yes_ask=11, no_bid=89, no_ask=91, volume=50000),
            FedMarket(ticker="FEDZ24-1", title="Exactly 1 Fed cut by Dec 2025", cuts=1, probability=0.20,
                     price=20, yes_bid=19, yes_ask=21, no_bid=79, no_ask=81, volume=75000),
            FedMarket(ticker="FEDZ24-2", title="Exactly 2 Fed cuts by Dec 2025", cuts=2, probability=0.30,
                     price=30, yes_bid=29, yes_ask=31, no_bid=69, no_ask=71, volume=100000),
            FedMarket(ticker="FEDZ24-3", title="Exactly 3 Fed cuts by Dec 2025", cuts=3, probability=0.25,
                     price=25, yes_bid=24, yes_ask=26, no_bid=74, no_ask=76, volume=60000),
            FedMarket(ticker="FEDZ24-4", title="4 or more Fed cuts by Dec 2025", cuts=4, probability=0.10,
                     price=10, yes_bid=9, yes_ask=11, no_bid=89, no_ask=91, volume=30000),
            FedMarket(ticker="FEDZ24-5", title="5 or more Fed cuts by Dec 2025", cuts=5, probability=0.05,
                     price=5, yes_bid=4, yes_ask=6, no_bid=94, no_ask=96, volume=15000),
        ]
    
    # Sort by number of cuts
    fed_markets.sort(key=lambda m: m.cuts)
    
    return fed_markets

@app.post("/api/calculate-hedge", response_model=HedgeResponse)
def calculate_hedge(request: HedgeRequest):
    """Calculate optimal hedge strategy based on mortgage details"""
    
    # Constants
    KALSHI_FEE_RATE = 0.07  # 7 cents per dollar
    FEE_ADJUSTMENT = 1.1
    
    # Get Fed markets from request or fetch them
    if request.fed_markets:
        fed_markets = request.fed_markets
    else:
        # Fetch current Fed markets
        fed_markets = asyncio.run(get_fed_markets())
        fed_markets = [m.dict() for m in fed_markets]
    
    # Calculate cuts needed to refinance
    rate_diff = request.current_rate - request.refinance_threshold
    cuts_to_refinance = math.ceil(rate_diff / 0.25)
    
    # Calculate yearly opportunity cost
    r1 = request.current_rate / 100 / 12
    r2 = request.refinance_threshold / 100 / 12
    n = 30 * 12
    
    current_payment = request.loan_amount * (r1 * pow(1 + r1, n)) / (pow(1 + r1, n) - 1)
    refi_payment = request.loan_amount * (r2 * pow(1 + r2, n)) / (pow(1 + r2, n) - 1)
    
    monthly_savings = current_payment - refi_payment
    yearly_opportunity_cost = monthly_savings * 12
    
    # Calculate optimal hedge amount
    optimal_hedge_amount = yearly_opportunity_cost * FEE_ADJUSTMENT
    
    # Get eligible markets (only bet on scenarios where we DON'T refinance)
    eligible_markets = [m for m in fed_markets if m.get('cuts', 0) < cuts_to_refinance]
    total_probability = sum(m.get('probability', m.get('price', 0)/100) for m in eligible_markets)
    
    # Calculate allocations based on strategy
    allocations = []
    
    if request.strategy_type == 'probability-weighted':
        for market in eligible_markets:
            prob = market.get('probability', market.get('price', 0)/100)
            weight = prob / total_probability if total_probability > 0 else 0
            allocation = optimal_hedge_amount * weight
            
            price_per_contract = market.get('price', 0) / 100
            contracts = int(allocation / price_per_contract) if price_per_contract > 0 else 0
            
            if contracts > 0:
                cost = contracts * price_per_contract
                fees = contracts * KALSHI_FEE_RATE
                payout = contracts * 1.0
                net_profit = payout - cost - fees
                
                allocations.append({
                    'cuts': market.get('cuts', 0),
                    'probability': prob,
                    'price': market.get('price', 0),
                    'ticker': market.get('ticker', ''),
                    'title': market.get('title', ''),
                    'allocation': allocation,
                    'weight': weight,
                    'contracts': contracts,
                    'cost': cost,
                    'fees': fees,
                    'payout': payout,
                    'netProfit': net_profit,
                    'kalshiUrl': f"https://kalshi.com/markets/{market.get('ticker', '')}"
                })
    
    elif request.strategy_type == 'max-protection':
        if eligible_markets:
            most_likely = max(eligible_markets, key=lambda m: m.get('probability', m.get('price', 0)/100))
            most_likely_prob = most_likely.get('probability', most_likely.get('price', 0)/100)
            
            for market in eligible_markets:
                if market == most_likely:
                    weight = 0.6
                else:
                    other_prob = total_probability - most_likely_prob
                    prob = market.get('probability', market.get('price', 0)/100)
                    weight = 0.4 * (prob / other_prob) if other_prob > 0 else 0
                
                allocation = optimal_hedge_amount * weight
                price_per_contract = market.get('price', 0) / 100
                contracts = int(allocation / price_per_contract) if price_per_contract > 0 else 0
                
                if contracts > 0:
                    cost = contracts * price_per_contract
                    fees = contracts * KALSHI_FEE_RATE
                    payout = contracts * 1.0
                    net_profit = payout - cost - fees
                    
                    allocations.append({
                        'cuts': market.get('cuts', 0),
                        'probability': market.get('probability', market.get('price', 0)/100),
                        'price': market.get('price', 0),
                        'ticker': market.get('ticker', ''),
                        'title': market.get('title', ''),
                        'allocation': allocation,
                        'weight': weight,
                        'contracts': contracts,
                        'cost': cost,
                        'fees': fees,
                        'payout': payout,
                        'netProfit': net_profit,
                        'kalshiUrl': f"https://kalshi.com/markets/{market.get('ticker', '')}"
                    })
    
    elif request.strategy_type == 'equal':
        if eligible_markets:
            weight = 1.0 / len(eligible_markets)
            allocation = optimal_hedge_amount * weight
            
            for market in eligible_markets:
                price_per_contract = market.get('price', 0) / 100
                contracts = int(allocation / price_per_contract) if price_per_contract > 0 else 0
                
                if contracts > 0:
                    cost = contracts * price_per_contract
                    fees = contracts * KALSHI_FEE_RATE
                    payout = contracts * 1.0
                    net_profit = payout - cost - fees
                    
                    allocations.append({
                        'cuts': market.get('cuts', 0),
                        'probability': market.get('probability', market.get('price', 0)/100),
                        'price': market.get('price', 0),
                        'ticker': market.get('ticker', ''),
                        'title': market.get('title', ''),
                        'allocation': allocation,
                        'weight': weight,
                        'contracts': contracts,
                        'cost': cost,
                        'fees': fees,
                        'payout': payout,
                        'netProfit': net_profit,
                        'kalshiUrl': f"https://kalshi.com/markets/{market.get('ticker', '')}"
                    })
    
    # Calculate total hedge cost and expected value
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

@app.get("/markets")
def get_all_markets():
    """Get all markets (for compatibility)"""
    with SessionLocal() as s:
        subq = (
            select(Quote.ticker, func.max(Quote.ts).label("max_ts"))
            .group_by(Quote.ticker)
            .subquery()
        )
        q = (
            select(
                Market.ticker, Market.title, Market.category, Market.last_update,
                Quote.yes_last, Quote.no_last, Quote.volume
            )
            .join(subq, subq.c.ticker == Market.ticker, isouter=True)
            .join(Quote, (Quote.ticker == subq.c.ticker) & (Quote.ts == subq.c.max_ts), isouter=True)
            .order_by(Market.ticker)
        )
        rows = s.execute(q).all()
        out = []
        for row in rows:
            ticker, title, category, last_update, yes_last, no_last, volume = row
            out.append({
                "ticker": ticker,
                "title": title,
                "category": category,
                "last_update": last_update.isoformat() if last_update else None,
                "yes_price": yes_last or 0,
                "no_price": no_last or 0,
                "volume_24h": volume or 0
            })
        return out

# ---------- Background Data Puller ----------
async def pull_kalshi_data():
    """Pull live data from Kalshi API"""
    if not kalshi_client:
        print("Kalshi client not available, using mock data")
        return
    
    try:
        # Fetch all markets
        markets = await kalshi_client.get_markets(limit=200)
        
        with SessionLocal() as s:
            for market_data in markets:
                ticker = market_data.get('ticker')
                title = market_data.get('title', '')
                
                # Check if it's a Fed market
                is_fed = is_fed_rate_market(title, ticker)
                rate_cuts = extract_rate_cuts(title, ticker) if is_fed else None
                
                # Upsert market
                market = s.get(Market, ticker)
                if not market:
                    market = Market(
                        ticker=ticker,
                        title=title,
                        category="Fed" if is_fed else market_data.get('category', 'General'),
                        is_fed_market=is_fed,
                        rate_cuts=rate_cuts,
                        last_update=datetime.now(timezone.utc)
                    )
                    s.add(market)
                else:
                    market.title = title
                    market.is_fed_market = is_fed
                    market.rate_cuts = rate_cuts
                    market.last_update = datetime.now(timezone.utc)
                
                # Get latest quote data
                orderbook = await kalshi_client.get_orderbook(ticker)
                
                # Extract bid/ask prices
                yes_side = orderbook.get('yes', [])
                no_side = orderbook.get('no', [])
                
                yes_bid = yes_side[0]['price'] if yes_side else 0
                yes_ask = yes_side[-1]['price'] if yes_side else 0
                no_bid = no_side[0]['price'] if no_side else 0
                no_ask = no_side[-1]['price'] if no_side else 0
                
                # Get last trade price
                trades = await kalshi_client.get_trades(ticker, limit=1)
                yes_last = trades[0]['yes_price'] if trades else yes_bid
                no_last = trades[0]['no_price'] if trades else no_bid
                volume = market_data.get('volume', 0)
                
                # Add quote
                quote = Quote(
                    ticker=ticker,
                    ts=datetime.now(timezone.utc),
                    yes_bid=yes_bid,
                    yes_ask=yes_ask,
                    no_bid=no_bid,
                    no_ask=no_ask,
                    yes_last=yes_last,
                    no_last=no_last,
                    volume=volume
                )
                s.add(quote)
            
            s.commit()
            
        # Broadcast Fed market updates
        fed_markets = await get_fed_markets()
        await broadcast({
            'type': 'fed-update',
            'markets': [m.dict() for m in fed_markets],
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
    except Exception as e:
        print(f"Error pulling Kalshi data: {e}")

# ---------- Mock Data Generator (Fallback) ----------
import random

def generate_mock_data():
    """Generate mock Fed market data when Kalshi API is not available"""
    with SessionLocal() as s:
        # Check if we have Fed markets
        fed_count = s.execute(
            select(func.count()).select_from(Market).where(Market.is_fed_market == True)
        ).scalar()
        
        if fed_count == 0:
            # Create mock Fed markets
            fed_markets = [
                Market(ticker="FEDZ24", title="No Fed cuts by Dec 2025", category="Fed", 
                      is_fed_market=True, rate_cuts=0),
                Market(ticker="FEDZ24-1", title="Exactly 1 Fed cut by Dec 2025", category="Fed",
                      is_fed_market=True, rate_cuts=1),
                Market(ticker="FEDZ24-2", title="Exactly 2 Fed cuts by Dec 2025", category="Fed",
                      is_fed_market=True, rate_cuts=2),
                Market(ticker="FEDZ24-3", title="Exactly 3 Fed cuts by Dec 2025", category="Fed",
                      is_fed_market=True, rate_cuts=3),
                Market(ticker="FEDZ24-4", title="4 or more Fed cuts by Dec 2025", category="Fed",
                      is_fed_market=True, rate_cuts=4),
                Market(ticker="FEDZ24-5", title="5 or more Fed cuts by Dec 2025", category="Fed",
                      is_fed_market=True, rate_cuts=5),
            ]
            s.add_all(fed_markets)
            s.commit()
        
        # Generate realistic quotes for Fed markets
        markets = s.execute(
            select(Market).where(Market.is_fed_market == True)
        ).scalars().all()
        
        for market in markets:
            # Generate realistic probabilities based on cuts
            if market.rate_cuts == 0:
                yes_price = random.randint(8, 15)
            elif market.rate_cuts == 1:
                yes_price = random.randint(18, 25)
            elif market.rate_cuts == 2:
                yes_price = random.randint(25, 35)
            elif market.rate_cuts == 3:
                yes_price = random.randint(20, 30)
            elif market.rate_cuts == 4:
                yes_price = random.randint(8, 15)
            else:
                yes_price = random.randint(3, 8)
            
            # Add some volatility
            yes_price = max(1, min(99, yes_price + random.randint(-2, 2)))
            no_price = 100 - yes_price
            
            # Generate bid/ask spread
            spread = random.randint(1, 2)
            yes_bid = max(1, yes_price - spread)
            yes_ask = min(99, yes_price + spread)
            no_bid = max(1, no_price - spread)
            no_ask = min(99, no_price + spread)
            
            # Generate volume
            volume = random.randint(10000, 100000)
            
            quote = Quote(
                ticker=market.ticker,
                ts=datetime.now(timezone.utc),
                yes_bid=yes_bid,
                yes_ask=yes_ask,
                no_bid=no_bid,
                no_ask=no_ask,
                yes_last=yes_price,
                no_last=no_price,
                volume=volume
            )
            s.add(quote)
            
            market.last_update = datetime.now(timezone.utc)
        
        s.commit()

async def pull_loop():
    """Main loop for pulling data"""
    while True:
        if kalshi_client:
            await pull_kalshi_data()
        else:
            generate_mock_data()
            
            # Broadcast mock Fed market updates
            fed_markets = await get_fed_markets()
            await broadcast({
                'type': 'fed-update',
                'markets': [m.dict() for m in fed_markets],
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        
        await asyncio.sleep(PULL_INTERVAL_SECONDS)

# ---------- Startup ----------
@app.on_event("startup")
async def on_startup():
    init_db()
    
    # Initialize with mock data if no Kalshi connection
    if not kalshi_client:
        generate_mock_data()
    
    # Start background data puller
    asyncio.create_task(pull_loop())
    
    print(f"Server started. Kalshi connected: {kalshi_client is not None}")

@app.on_event("shutdown")
async def on_shutdown():
    if kalshi_client:
        kalshi_client.close()
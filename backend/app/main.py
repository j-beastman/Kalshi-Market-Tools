import os
import json
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from sqlalchemy import create_engine, Column, String, Integer, BigInteger, DateTime, ForeignKey, select, func
from sqlalchemy.orm import sessionmaker, DeclarativeBase, mapped_column, Mapped

# ---------- Config ----------
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./dev.db")
PULL_INTERVAL_SECONDS = int(os.getenv("PULL_INTERVAL_SECONDS", "15"))

# ---------- DB setup ----------
class Base(DeclarativeBase):
    pass

class Market(Base):
    __tablename__ = "markets"
    ticker: Mapped[str] = mapped_column(String, primary_key=True)
    title: Mapped[str] = mapped_column(String, nullable=False)
    category: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    last_update: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Additional fields for Fed rate markets
    rate_cuts: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    expiry_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

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

# ---------- App ----------
app = FastAPI(title="Kalshi Pipeline API", version="0.2.0")

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
class MarketOut(BaseModel):
    ticker: str
    title: str
    category: Optional[str] = None
    last_update: Optional[datetime] = None
    yes_price: Optional[int] = 0
    no_price: Optional[int] = 0
    volume_24h: Optional[int] = 0
    yes_bid: Optional[int] = 0
    yes_ask: Optional[int] = 0
    no_bid: Optional[int] = 0
    no_ask: Optional[int] = 0

class FedRateMarket(BaseModel):
    cuts: int
    probability: float
    price: int
    yes_bid: int
    yes_ask: int
    no_bid: int
    no_ask: int
    volume: int
    ticker: str
    title: str
    expiry_date: Optional[str] = None

class HedgeRequest(BaseModel):
    loan_amount: float
    current_rate: float
    refinance_threshold: float
    refinance_cost: float
    hedge_amount: float

class HedgeResponse(BaseModel):
    potential_loss: float
    cuts_to_refinance: int
    strategies: List[Dict[str, Any]]
    scenarios: Dict[str, Any]
    expected_value: float

# ---------- Routes ----------
@app.get("/health")
def health():
    return {"ok": True, "time": datetime.now(timezone.utc).isoformat()}

@app.get("/markets", response_model=List[MarketOut])
def get_markets():
    with SessionLocal() as s:
        subq = (
            select(Quote.ticker, func.max(Quote.ts).label("max_ts"))
            .group_by(Quote.ticker)
            .subquery()
        )
        q = (
            select(
                Market.ticker, Market.title, Market.category, Market.last_update,
                Quote.yes_last, Quote.no_last,
                Quote.yes_bid, Quote.yes_ask, Quote.no_bid, Quote.no_ask,
                Quote.volume
            )
            .join(subq, subq.c.ticker == Market.ticker, isouter=True)
            .join(Quote, (Quote.ticker == subq.c.ticker) & (Quote.ts == subq.c.max_ts), isouter=True)
            .order_by(Market.ticker)
        )
        rows = s.execute(q).all()
        out = []
        for row in rows:
            ticker, title, category, last_update, yes_last, no_last, yes_bid, yes_ask, no_bid, no_ask, volume = row
            out.append(
                MarketOut(
                    ticker=ticker, title=title, category=category, last_update=last_update,
                    yes_price=yes_last or 0, no_price=no_last or 0, volume_24h=volume or 0,
                    yes_bid=yes_bid or 0, yes_ask=yes_ask or 0,
                    no_bid=no_bid or 0, no_ask=no_ask or 0
                ).model_dump()
            )
        return out

@app.get("/markets/{ticker}")
def get_market_detail(ticker: str):
    with SessionLocal() as s:
        m = s.get(Market, ticker)
        if not m:
            return JSONResponse({"error": "not found"}, status_code=404)
        q = (
            select(Quote.ts, Quote.yes_last, Quote.no_last, Quote.volume)
            .where(Quote.ticker == ticker)
            .order_by(Quote.ts.desc())
            .limit(500)
        )
        rows = s.execute(q).all()
        return {
            "ticker": ticker,
            "title": m.title,
            "category": m.category,
            "series": [{"ts": ts.isoformat(), "yes": y or 0, "no": n or 0, "vol": v or 0} for ts, y, n, v in rows[::-1]]
        }

@app.get("/fed-rates", response_model=List[FedRateMarket])
def get_fed_rate_markets():
    """Get all Fed rate cut markets"""
    with SessionLocal() as s:
        # Get all markets in Fed category
        markets = s.query(Market).filter(Market.category == "Fed").all()
        
        result = []
        for m in markets:
            # Get latest quote
            latest = s.execute(
                select(Quote).where(Quote.ticker == m.ticker).order_by(Quote.ts.desc()).limit(1)
            ).scalars().first()
            
            if latest:
                # Extract number of cuts from title or ticker
                cuts = m.rate_cuts if m.rate_cuts is not None else _extract_cuts_from_title(m.title)
                
                result.append(FedRateMarket(
                    cuts=cuts,
                    probability=latest.yes_last / 100.0 if latest.yes_last else 0,
                    price=latest.yes_last or 0,
                    yes_bid=latest.yes_bid or max(0, (latest.yes_last or 0) - 1),
                    yes_ask=latest.yes_ask or min(100, (latest.yes_last or 0) + 1),
                    no_bid=latest.no_bid or max(0, (latest.no_last or 0) - 1),
                    no_ask=latest.no_ask or min(100, (latest.no_last or 0) + 1),
                    volume=latest.volume or 0,
                    ticker=m.ticker,
                    title=m.title,
                    expiry_date=m.expiry_date.isoformat() if m.expiry_date else None
                ))
        
        # Sort by number of cuts
        result.sort(key=lambda x: x.cuts)
        return result

@app.post("/calculate-hedge", response_model=HedgeResponse)
def calculate_hedge(request: HedgeRequest):
    """Calculate optimal hedge strategy"""
    
    # Get Fed rate markets
    with SessionLocal() as s:
        markets = s.query(Market).filter(Market.category == "Fed").all()
        
        market_data = []
        for m in markets:
            latest = s.execute(
                select(Quote).where(Quote.ticker == m.ticker).order_by(Quote.ts.desc()).limit(1)
            ).scalars().first()
            
            if latest:
                cuts = m.rate_cuts if m.rate_cuts is not None else _extract_cuts_from_title(m.title)
                market_data.append({
                    'cuts': cuts,
                    'probability': latest.yes_last / 100.0 if latest.yes_last else 0,
                    'price': latest.yes_last or 0,
                    'ticker': m.ticker
                })
    
    # Sort by cuts
    market_data.sort(key=lambda x: x['cuts'])
    
    # Calculate cuts needed to refinance
    rate_diff = request.current_rate - request.refinance_threshold
    cuts_to_refinance = max(0, int(rate_diff / 0.25))
    
    # Calculate potential loss (simplified)
    r1 = request.current_rate / 100 / 12
    r2 = request.refinance_threshold / 100 / 12
    n = 30 * 12
    
    payment1 = request.loan_amount * (r1 * pow(1 + r1, n)) / (pow(1 + r1, n) - 1)
    payment2 = request.loan_amount * (r2 * pow(1 + r2, n)) / (pow(1 + r2, n) - 1)
    monthly_savings = payment1 - payment2
    potential_loss = monthly_savings * 12 * 5  # 5 years of savings
    
    # Calculate hedge strategies
    strategies = []
    total_hedge_cost = 0
    
    KALSHI_FEE = 0.07  # 7 cents total per contract
    
    for market in market_data:
        if market['cuts'] < cuts_to_refinance:
            price_per_contract = market['price'] / 100
            contracts_needed = int(request.hedge_amount / price_per_contract) if price_per_contract > 0 else 0
            
            if contracts_needed > 0:
                total_cost = contracts_needed * price_per_contract
                fees = contracts_needed * KALSHI_FEE
                payout = contracts_needed * 1.0
                net_profit = payout - total_cost - fees
                
                strategies.append({
                    'cuts': market['cuts'],
                    'probability': market['probability'],
                    'price': market['price'],
                    'contracts': contracts_needed,
                    'cost': total_cost,
                    'fees': fees,
                    'payout': payout,
                    'net_profit': net_profit,
                    'ticker': market['ticker']
                })
                
                total_hedge_cost += total_cost + fees
    
    # Calculate scenarios
    no_refi_prob = sum(s['probability'] for s in strategies)
    refi_prob = 1 - no_refi_prob
    
    expected_hedge_value = sum(s['probability'] * s['net_profit'] for s in strategies)
    expected_refi_value = refi_prob * (potential_loss - request.refinance_cost - total_hedge_cost)
    total_expected_value = expected_hedge_value + expected_refi_value
    
    scenarios = {
        'no_refinance': {
            'probability': no_refi_prob,
            'expected_profit': expected_hedge_value,
            'outcomes': strategies
        },
        'refinance': {
            'probability': refi_prob,
            'hedge_loss': -total_hedge_cost,
            'refinance_savings': potential_loss - request.refinance_cost,
            'net_outcome': potential_loss - request.refinance_cost - total_hedge_cost
        },
        'hedge_cost': total_hedge_cost,
        'expected_hedge_value': expected_hedge_value,
        'expected_refi_value': expected_refi_value,
        'total_expected_value': total_expected_value
    }
    
    return HedgeResponse(
        potential_loss=potential_loss,
        cuts_to_refinance=cuts_to_refinance,
        strategies=strategies,
        scenarios=scenarios,
        expected_value=total_expected_value
    )

def _extract_cuts_from_title(title: str) -> int:
    """Extract number of rate cuts from market title"""
    # Simple extraction - in production, use regex or NLP
    if "no cut" in title.lower() or "0 cut" in title.lower():
        return 0
    elif "1 cut" in title.lower() or "one cut" in title.lower():
        return 1
    elif "2 cut" in title.lower() or "two cut" in title.lower():
        return 2
    elif "3 cut" in title.lower() or "three cut" in title.lower():
        return 3
    elif "4 cut" in title.lower() or "four cut" in title.lower():
        return 4
    elif "5 cut" in title.lower() or "five cut" in title.lower():
        return 5
    else:
        return 0

# ---------- Background puller (with Fed markets) ----------
import random

def _seed_if_empty():
    with SessionLocal() as s:
        if s.query(Market).count() == 0:
            # Seed Fed rate cut markets
            fed_markets = [
                Market(
                    ticker="FED-NOCUT-2025", 
                    title="Fed makes no rate cuts by end of 2025", 
                    category="Fed",
                    rate_cuts=0,
                    last_update=datetime.now(timezone.utc)
                ),
                Market(
                    ticker="FED-1CUT-2025", 
                    title="Fed makes exactly 1 rate cut by end of 2025", 
                    category="Fed",
                    rate_cuts=1,
                    last_update=datetime.now(timezone.utc)
                ),
                Market(
                    ticker="FED-2CUT-2025", 
                    title="Fed makes exactly 2 rate cuts by end of 2025", 
                    category="Fed",
                    rate_cuts=2,
                    last_update=datetime.now(timezone.utc)
                ),
                Market(
                    ticker="FED-3CUT-2025", 
                    title="Fed makes exactly 3 rate cuts by end of 2025", 
                    category="Fed",
                    rate_cuts=3,
                    last_update=datetime.now(timezone.utc)
                ),
                Market(
                    ticker="FED-4CUT-2025", 
                    title="Fed makes 4 or more rate cuts by end of 2025", 
                    category="Fed",
                    rate_cuts=4,
                    last_update=datetime.now(timezone.utc)
                ),
            ]
            
            # Add other sample markets
            other_markets = [
                Market(ticker="POTUS-2024", title="Who wins the 2024 U.S. Presidential Election?", category="Politics", last_update=datetime.now(timezone.utc)),
            ]
            
            s.add_all(fed_markets + other_markets)
            s.commit()

def pull_once():
    """Simulate market updates with realistic Fed probabilities"""
    now = datetime.now(timezone.utc)
    with SessionLocal() as s:
        for m in s.query(Market).all():
            if m.category == "Fed":
                # Realistic Fed rate probabilities
                if m.rate_cuts == 0:
                    y = random.randint(12, 18)  # 12-18% for no cuts
                elif m.rate_cuts == 1:
                    y = random.randint(22, 28)  # 22-28% for 1 cut
                elif m.rate_cuts == 2:
                    y = random.randint(28, 35)  # 28-35% for 2 cuts
                elif m.rate_cuts == 3:
                    y = random.randint(18, 25)  # 18-25% for 3 cuts
                else:
                    y = random.randint(5, 10)   # 5-10% for 4+ cuts
                    
                # Add some volatility
                y = max(1, min(99, y + random.randint(-2, 2)))
                n = 100 - y
                
                # Realistic bid/ask spreads
                spread = random.randint(1, 3)
                yes_bid = max(1, y - spread)
                yes_ask = min(99, y + spread)
                no_bid = max(1, n - spread)
                no_ask = min(99, n + spread)
                
                vol = random.randint(1000, 50000)  # Higher volume for Fed markets
                
                s.add(Quote(
                    ticker=m.ticker, 
                    ts=now, 
                    yes_last=y, 
                    no_last=n,
                    yes_bid=yes_bid,
                    yes_ask=yes_ask,
                    no_bid=no_bid,
                    no_ask=no_ask,
                    volume=vol
                ))
            else:
                # Regular markets
                y = random.randint(30, 70)
                n = 100 - y
                vol = random.randint(0, 5000)
                s.add(Quote(ticker=m.ticker, ts=now, yes_last=y, no_last=n, volume=vol))
            
            m.last_update = now
        s.commit()

async def pull_loop():
    while True:
        pull_once()
        with SessionLocal() as s:
            out = []
            for m in s.query(Market).all():
                latest = s.execute(
                    select(Quote).where(Quote.ticker == m.ticker).order_by(Quote.ts.desc()).limit(1)
                ).scalars().first()
                if latest:
                    out.append({
                        "ticker": m.ticker,
                        "yes_price": latest.yes_last,
                        "no_price": latest.no_last,
                        "yes_bid": latest.yes_bid,
                        "yes_ask": latest.yes_ask,
                        "no_bid": latest.no_bid,
                        "no_ask": latest.no_ask,
                        "volume_24h": latest.volume
                    })
        await broadcast({"type": "upsert", "markets": out, "ts": datetime.now(timezone.utc).isoformat()})
        await asyncio.sleep(PULL_INTERVAL_SECONDS)

# ---------- Lifespan ----------
@app.on_event("startup")
async def on_startup():
    init_db()
    _seed_if_empty()
    scheduler = AsyncIOScheduler()
    scheduler.add_job(pull_once, "interval", seconds=PULL_INTERVAL_SECONDS)
    scheduler.start()
    asyncio.create_task(pull_loop())
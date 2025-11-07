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

class MortgageHedgeRequest(BaseModel):
    principal: float
    current_rate: float
    years_remaining: int
    target_rate: float
    total_hedge_amount: Optional[float] = None
    allocations: Optional[Dict[str, float]] = None

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
                Quote.volume
            )
            .join(subq, subq.c.ticker == Market.ticker, isouter=True)
            .join(Quote, (Quote.ticker == subq.c.ticker) & (Quote.ts == subq.c.max_ts), isouter=True)
            .order_by(Market.ticker)
        )
        rows = s.execute(q).all()
        out = []
        for row in rows:
            ticker, title, category, last_update, yes_last, no_last, volume = row
            out.append(
                MarketOut(
                    ticker=ticker, title=title, category=category, last_update=last_update,
                    yes_price=yes_last or 0, no_price=no_last or 0, volume_24h=volume or 0
                ).model_dump()
            )
        return out

@app.get("/fed-markets")
def get_fed_markets():
    """
    Get Fed rate cut markets for hedging calculations.
    Filters for KXRATECUTCOUNT series and ensures good liquidity.
    """
    with SessionLocal() as s:
        # Get latest quotes for KXRATECUTCOUNT markets
        subq = (
            select(Quote.ticker, func.max(Quote.ts).label("max_ts"))
            .group_by(Quote.ticker)
            .subquery()
        )
        
        # Query for Fed rate cut markets with liquidity data
        q = (
            select(
                Market.ticker, 
                Market.title, 
                Market.category, 
                Market.last_update,
                Quote.yes_last, 
                Quote.no_last, 
                Quote.volume,
                Quote.yes_bid, 
                Quote.yes_ask, 
                Quote.no_bid, 
                Quote.no_ask
            )
            .join(subq, subq.c.ticker == Market.ticker, isouter=True)
            .join(Quote, (Quote.ticker == subq.c.ticker) & (Quote.ts == subq.c.max_ts), isouter=True)
            .where(Market.ticker.like('KXRATECUTCOUNT%'))
            .order_by(Quote.volume.desc().nullslast())
        )
        rows = s.execute(q).all()
        
        # Process and filter markets
        MIN_VOLUME = 100  # Minimum volume for liquidity
        fed_markets = []
        
        for row in rows:
            ticker, title, category, last_update, yes_last, no_last, volume, yes_bid, yes_ask, no_bid, no_ask = row
            
            # Skip if no volume data
            if not volume or volume < MIN_VOLUME:
                continue
            
            # Extract number of cuts from ticker
            cuts = 0
            if '-' in ticker:
                try:
                    parts = ticker.split('-')
                    if len(parts) >= 2:
                        cuts = int(parts[-1])
                except:
                    cuts = 0
            
            # Calculate spread for liquidity assessment
            spread = 0
            if yes_bid and yes_ask:
                spread = yes_ask - yes_bid
            elif no_bid and no_ask:
                spread = no_ask - no_bid
            
            # Calculate liquidity score (0-100)
            volume_score = min(50, (volume / 1000) * 25)  # Up to 50 points for volume
            spread_score = max(0, 50 - spread * 2)  # Up to 50 points for tight spread
            liquidity_score = volume_score + spread_score
            
            fed_markets.append({
                "ticker": ticker,
                "title": title or f"Fed cuts rates {cuts} time{'s' if cuts != 1 else ''} by Dec 2025",
                "cuts": cuts,
                "yes_price": yes_last or 50,
                "no_price": no_last or 50,
                "yes_bid": yes_bid or (yes_last - 1 if yes_last else 49),
                "yes_ask": yes_ask or (yes_last + 1 if yes_last else 51),
                "no_bid": no_bid or (no_last - 1 if no_last else 49),
                "no_ask": no_ask or (no_last + 1 if no_last else 51),
                "volume": volume,
                "spread": spread,
                "liquidity_score": round(liquidity_score, 1)
            })
        
        # Sort by liquidity score (best liquidity first)
        fed_markets.sort(key=lambda x: x['liquidity_score'], reverse=True)
        
        # Return top 10 most liquid markets
        return {
            "markets": fed_markets[:10],
            "count": len(fed_markets[:10]),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@app.post("/calculate-hedge")
def calculate_hedge(request: MortgageHedgeRequest):
    """
    Calculate potential savings from hedging strategy.
    This endpoint processes mortgage details and returns hedge recommendations.
    """
    # Calculate monthly payment savings
    months = request.years_remaining * 12
    monthly_rate_current = request.current_rate / 100 / 12
    monthly_rate_target = request.target_rate / 100 / 12
    
    # Current monthly payment
    if monthly_rate_current > 0:
        current_payment = request.principal * (
            monthly_rate_current * (1 + monthly_rate_current) ** months
        ) / ((1 + monthly_rate_current) ** months - 1)
    else:
        current_payment = request.principal / months
    
    # Target monthly payment
    if monthly_rate_target > 0:
        target_payment = request.principal * (
            monthly_rate_target * (1 + monthly_rate_target) ** months
        ) / ((1 + monthly_rate_target) ** months - 1)
    else:
        target_payment = request.principal / months
    
    # Calculate savings
    monthly_savings = current_payment - target_payment
    yearly_savings = monthly_savings * 12
    total_savings = monthly_savings * months
    
    # Calculate rate cuts needed (assuming 25bps per cut affects mortgage rates by ~20bps)
    bps_difference = (request.current_rate - request.target_rate) * 100
    cuts_needed = int(bps_difference / 20)  # Rough estimate
    
    # Default hedge amount if not provided
    if not request.total_hedge_amount:
        request.total_hedge_amount = yearly_savings  # Hedge 1 year of savings
    
    return {
        "current_payment": round(current_payment, 2),
        "target_payment": round(target_payment, 2),
        "monthly_savings": round(monthly_savings, 2),
        "yearly_savings": round(yearly_savings, 2),
        "total_savings": round(total_savings, 2),
        "cuts_needed": cuts_needed,
        "recommended_hedge": round(request.total_hedge_amount, 2),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

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

# ---------- Background puller with Fed markets simulation ----------
import random

def _seed_if_empty():
    with SessionLocal() as s:
        # Add regular markets
        if s.get(Market, "RATE-CUT-DEC") is None:
            s.add(Market(ticker="RATE-CUT-DEC", title="Fed cuts rates in December?", category="Fed", last_update=datetime.now(timezone.utc)))
            s.commit()
        
        # Add KXRATECUTCOUNT series markets
        for cuts in range(0, 8):
            ticker = f"KXRATECUTCOUNT-{cuts}"
            if s.get(Market, ticker) is None:
                title = f"Fed cuts rates {cuts} time{'s' if cuts != 1 else ''} by Dec 2025"
                s.add(Market(
                    ticker=ticker, 
                    title=title, 
                    category="Fed Rate Cuts", 
                    last_update=datetime.now(timezone.utc)
                ))
        s.commit()

def pull_once():
    """Simulate market data updates including Fed markets"""
    now = datetime.now(timezone.utc)
    with SessionLocal() as s:
        for m in s.query(Market).all():
            # Different pricing logic for Fed cut markets
            if m.ticker.startswith("KXRATECUTCOUNT"):
                try:
                    cuts = int(m.ticker.split('-')[-1])
                    # Price based on number of cuts (fewer cuts = higher NO price)
                    base_yes = max(10, 70 - (cuts * 10))  # More cuts = lower probability
                    yes_price = base_yes + random.randint(-5, 5)
                    yes_price = max(5, min(95, yes_price))
                    no_price = 100 - yes_price
                    
                    # Higher volume for more likely scenarios (1-3 cuts)
                    if 1 <= cuts <= 3:
                        vol = random.randint(500, 5000)
                    else:
                        vol = random.randint(100, 1000)
                    
                    # Tighter spreads for higher volume markets
                    spread = 1 if vol > 2000 else 2 if vol > 500 else 3
                    
                    s.add(Quote(
                        ticker=m.ticker, 
                        ts=now, 
                        yes_last=yes_price, 
                        no_last=no_price,
                        yes_bid=yes_price - spread,
                        yes_ask=yes_price + spread,
                        no_bid=no_price - spread,
                        no_ask=no_price + spread,
                        volume=vol
                    ))
                except:
                    pass
            else:
                # Regular market simulation
                y = random.randint(30, 70)
                n = 100 - y
                vol = random.randint(0, 5000)
                s.add(Quote(ticker=m.ticker, ts=now, yes_last=y, no_last=n, volume=vol))
            
            m.last_update = now
        s.commit()

async def pull_loop():
    while True:
        pull_once()
        # Broadcast updates
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
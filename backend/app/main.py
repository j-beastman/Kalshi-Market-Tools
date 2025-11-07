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
app = FastAPI(title="Kalshi Pipeline API", version="0.3.0")

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
    allocations: Optional[Dict[str, Dict[str, Any]]] = None  # { ticker: { side: 'yes'|'no', amount: 100 } }

class ScenarioAnalysisRequest(BaseModel):
    mortgage: Dict[str, Any]
    allocations: Dict[str, Dict[str, Any]]
    fed_markets: List[Dict[str, Any]]

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
    Returns markets sorted by number of cuts for easy organization.
    """
    with SessionLocal() as s:
        subq = (
            select(Quote.ticker, func.max(Quote.ts).label("max_ts"))
            .group_by(Quote.ticker)
            .subquery()
        )
        
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
            .order_by(Market.ticker)  # Sort by ticker to get cuts in order
        )
        rows = s.execute(q).all()
        
        MIN_VOLUME = 50  # Lower minimum for more markets
        fed_markets = []
        
        for row in rows:
            ticker, title, category, last_update, yes_last, no_last, volume, yes_bid, yes_ask, no_bid, no_ask = row
            
            # Extract number of cuts
            cuts = 0
            if '-' in ticker:
                try:
                    cuts = int(ticker.split('-')[-1])
                except:
                    cuts = 0
            
            # Calculate spread and liquidity
            spread = 0
            if yes_bid and yes_ask:
                spread = yes_ask - yes_bid
            elif no_bid and no_ask:
                spread = no_ask - no_bid
            
            # Include all markets with some volume
            if volume and volume >= MIN_VOLUME:
                volume_score = min(50, (volume / 1000) * 25)
                spread_score = max(0, 50 - spread * 2)
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
                    "liquidity_score": round(liquidity_score, 1),
                    "implied_probability": yes_last / 100 if yes_last else 0.5
                })
        
        # Sort by number of cuts (ascending) for logical organization
        fed_markets.sort(key=lambda x: x['cuts'])
        
        return {
            "markets": fed_markets,
            "count": len(fed_markets),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@app.post("/calculate-hedge")
def calculate_hedge(request: MortgageHedgeRequest):
    """
    Calculate potential savings and smart hedge allocation.
    """
    # Calculate monthly payments
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
    
    # Calculate rate cuts needed
    bps_difference = (request.current_rate - request.target_rate) * 100
    cuts_needed = int(bps_difference / 25)  # 25bps per cut
    
    # Default hedge amount
    if not request.total_hedge_amount:
        request.total_hedge_amount = yearly_savings
    
    # Smart allocation strategy
    allocation_strategy = {
        "threshold": cuts_needed,
        "no_bet_range": f"0-{cuts_needed-1} cuts",
        "yes_bet_range": f"{cuts_needed}-7 cuts",
        "no_allocation": request.total_hedge_amount * 0.6,
        "yes_allocation": request.total_hedge_amount * 0.4,
        "rationale": "60% NO protects against rates staying high, 40% YES captures upside if rates drop enough"
    }
    
    return {
        "current_payment": round(current_payment, 2),
        "target_payment": round(target_payment, 2),
        "monthly_savings": round(monthly_savings, 2),
        "yearly_savings": round(yearly_savings, 2),
        "total_savings": round(total_savings, 2),
        "cuts_needed": cuts_needed,
        "bps_difference": bps_difference,
        "recommended_hedge": round(request.total_hedge_amount, 2),
        "allocation_strategy": allocation_strategy,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.post("/scenario-analysis")
def scenario_analysis(request: ScenarioAnalysisRequest):
    """
    Analyze outcomes for each possible Fed rate scenario.
    """
    scenarios = []
    mortgage = request.mortgage
    allocations = request.allocations
    fed_markets = request.fed_markets
    
    principal = mortgage.get('principal', 400000)
    current_rate = mortgage.get('current_rate', 7.0)
    years_remaining = mortgage.get('years_remaining', 25)
    target_rate = mortgage.get('target_rate', 5.5)
    months = years_remaining * 12
    
    # Current monthly payment
    monthly_rate_current = current_rate / 100 / 12
    if monthly_rate_current > 0:
        current_payment = principal * (
            monthly_rate_current * (1 + monthly_rate_current) ** months
        ) / ((1 + monthly_rate_current) ** months - 1)
    else:
        current_payment = principal / months
    
    # Analyze each scenario
    for market in fed_markets:
        cuts = market['cuts']
        new_rate = max(3.0, current_rate - (cuts * 0.25))  # 25bps per cut
        
        # New payment at this rate
        new_monthly_rate = new_rate / 100 / 12
        if new_monthly_rate > 0:
            new_payment = principal * (
                new_monthly_rate * (1 + new_monthly_rate) ** months
            ) / ((1 + new_monthly_rate) ** months - 1)
        else:
            new_payment = principal / months
        
        # Calculate savings
        monthly_savings = current_payment - new_payment
        yearly_savings = monthly_savings * 12
        can_refinance = new_rate <= target_rate
        
        # Calculate hedge P&L
        hedge_pl = 0
        positions = []
        
        for ticker, alloc in allocations.items():
            # Find the market for this allocation
            hedge_market = next((m for m in fed_markets if m['ticker'] == ticker), None)
            if not hedge_market:
                continue
            
            side = alloc.get('side', 'none')
            amount = alloc.get('amount', 0)
            
            if side == 'none' or amount <= 0:
                continue
            
            # Determine if bet wins in this scenario
            bet_wins = False
            if cuts == hedge_market['cuts']:
                # This is the market that happens
                bet_wins = (side == 'yes')
            else:
                # This market doesn't happen
                bet_wins = (side == 'no')
            
            if bet_wins:
                price = hedge_market['yes_price'] if side == 'yes' else hedge_market['no_price']
                payout = amount / (price / 100)
                profit = payout - amount
                hedge_pl += profit
            else:
                hedge_pl -= amount
            
            positions.append({
                "ticker": ticker,
                "side": side,
                "amount": amount,
                "wins": bet_wins,
                "pl": profit if bet_wins else -amount
            })
        
        # Expected value
        probability = market['yes_price'] / 100
        expected_value = (yearly_savings + hedge_pl) * probability if can_refinance else hedge_pl * probability
        
        scenarios.append({
            "cuts": cuts,
            "new_rate": round(new_rate, 2),
            "probability": round(probability, 3),
            "can_refinance": can_refinance,
            "monthly_savings": round(monthly_savings, 2) if can_refinance else 0,
            "yearly_savings": round(yearly_savings, 2) if can_refinance else 0,
            "hedge_pl": round(hedge_pl, 2),
            "net_outcome": round(yearly_savings + hedge_pl if can_refinance else hedge_pl, 2),
            "expected_value": round(expected_value, 2),
            "positions": positions
        })
    
    # Calculate overall expected value
    total_ev = sum(s['expected_value'] for s in scenarios)
    
    # Find best and worst scenarios
    best_scenario = max(scenarios, key=lambda s: s['net_outcome'])
    worst_scenario = min(scenarios, key=lambda s: s['net_outcome'])
    
    return {
        "scenarios": scenarios,
        "total_expected_value": round(total_ev, 2),
        "best_scenario": {
            "cuts": best_scenario['cuts'],
            "outcome": best_scenario['net_outcome']
        },
        "worst_scenario": {
            "cuts": worst_scenario['cuts'],
            "outcome": worst_scenario['net_outcome']
        },
        "positive_scenarios": sum(1 for s in scenarios if s['net_outcome'] > 0),
        "negative_scenarios": sum(1 for s in scenarios if s['net_outcome'] < 0),
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

# ---------- Background puller with realistic Fed markets ----------
import random

def _seed_if_empty():
    with SessionLocal() as s:
        # Add KXRATECUTCOUNT markets with realistic probabilities
        probabilities = {
            0: 10,  # 10% chance of no cuts
            1: 20,  # 20% chance of 1 cut
            2: 30,  # 30% chance of 2 cuts (most likely)
            3: 25,  # 25% chance of 3 cuts
            4: 10,  # 10% chance of 4 cuts
            5: 3,   # 3% chance of 5 cuts
            6: 1,   # 1% chance of 6 cuts
            7: 1    # 1% chance of 7 cuts
        }
        
        for cuts, yes_prob in probabilities.items():
            ticker = f"KXRATECUTCOUNT-{cuts}"
            if s.get(Market, ticker) is None:
                if cuts == 1:
                    title = f"Fed cuts rates exactly 1 time by Dec 2025"
                else:
                    title = f"Fed cuts rates exactly {cuts} times by Dec 2025"
                s.add(Market(
                    ticker=ticker, 
                    title=title, 
                    category="Fed Rate Cuts", 
                    last_update=datetime.now(timezone.utc)
                ))
        s.commit()

def pull_once():
    """Simulate realistic Fed market data"""
    now = datetime.now(timezone.utc)
    
    # Realistic base probabilities
    base_probabilities = {
        0: 10, 1: 20, 2: 30, 3: 25, 4: 10, 5: 3, 6: 1, 7: 1
    }
    
    with SessionLocal() as s:
        for m in s.query(Market).all():
            if m.ticker.startswith("KXRATECUTCOUNT"):
                try:
                    cuts = int(m.ticker.split('-')[-1])
                    
                    # Base probability with some random variation
                    base_yes = base_probabilities.get(cuts, 5)
                    variation = random.randint(-3, 3)
                    yes_price = max(5, min(95, base_yes + variation))
                    no_price = 100 - yes_price
                    
                    # Higher volume for more likely scenarios
                    if 1 <= cuts <= 3:
                        vol = random.randint(2000, 10000)
                        spread = 1
                    elif cuts in [0, 4]:
                        vol = random.randint(500, 2000)
                        spread = 2
                    else:
                        vol = random.randint(100, 500)
                        spread = 3
                    
                    s.add(Quote(
                        ticker=m.ticker, 
                        ts=now, 
                        yes_last=yes_price, 
                        no_last=no_price,
                        yes_bid=max(1, yes_price - spread),
                        yes_ask=min(99, yes_price + spread),
                        no_bid=max(1, no_price - spread),
                        no_ask=min(99, no_price + spread),
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
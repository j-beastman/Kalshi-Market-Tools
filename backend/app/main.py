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
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "*")
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
app = FastAPI(title="Kalshi Pipeline API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN] if FRONTEND_ORIGIN != "*" else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- SSE subscribers ----------
_subscribers: List[asyncio.Queue] = []

async def broadcast(payload: Dict[str, Any]):
    # push to all subscribers
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

# ---------- Routes ----------
@app.get("/health")
def health():
    return {"ok": True, "time": datetime.now(timezone.utc).isoformat()}

@app.get("/markets", response_model=List[MarketOut])
def get_markets():
    with SessionLocal() as s:
        # latest quote per ticker
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

# ---------- Background puller (stub that simulates data without Kalshi creds) ----------
import random

def _seed_if_empty():
    with SessionLocal() as s:
        if s.get(Market, "RATE-CUT-DEC") is None:
            s.add_all([
                Market(ticker="RATE-CUT-DEC", title="Fed cuts rates in December?", category="Fed", last_update=datetime.now(timezone.utc)),
                Market(ticker="POTUS-2024", title="Who wins the 2024 U.S. Presidential Election?", category="Politics", last_update=datetime.now(timezone.utc)),
            ])
            s.commit()

def pull_once():
    # In production: call Kalshi API here and upsert
    # For now, simulate quotes to prove end-to-end
    now = datetime.now(timezone.utc)
    with SessionLocal() as s:
        for m in s.query(Market).all():
            y = random.randint(30, 70)
            n = 100 - y
            vol = random.randint(0, 5000)
            s.add(Quote(ticker=m.ticker, ts=now, yes_last=y, no_last=n, volume=vol))
            m.last_update = now
        s.commit()

async def pull_loop():
    while True:
        pull_once()
        # broadcast a compact patch for the UI
        with SessionLocal() as s:
            out = []
            for m in s.query(Market).all():
                # get latest quote for ticker m
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
    # Also kick off an async loop that broadcasts to SSE
    asyncio.create_task(pull_loop())

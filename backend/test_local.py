#!/usr/bin/env python
"""
Local testing script for Kalshi Market Tools API
Run this to test your backend locally before deploying to Railway
"""

import os
import sys
import asyncio
import logging
from pathlib import Path

# Add app directory to path
sys.path.insert(0, str(Path(__file__).parent / "app"))

from app.kalshi_client import KalshiClient
from app.database import Database
from app.market_analyzer import MarketAnalyzer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_database_connection():
    """Test PostgreSQL connection"""
    print("\n🔍 Testing Database Connection...")
    
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL not set. Using mock database.")
        return None
    
    try:
        db = Database(db_url)
        await db.initialize()
        stats = await db.get_statistics()
        print(f"✅ Database connected! Stats: {stats}")
        return db
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None


async def test_kalshi_api():
    """Test Kalshi API connection"""
    print("\n🔍 Testing Kalshi API Connection...")
    
    api_key = os.getenv("KALSHI_API_KEY")
    private_key = os.getenv("KALSHI_PRIVATE_KEY")
    
    if not api_key or not private_key:
        print("❌ Kalshi credentials not set. Running in mock mode.")
        return None
    
    try:
        client = KalshiClient(api_key, private_key)
        
        # Test getting markets
        markets = await client.get_markets(limit=5)
        print(f"✅ Kalshi API connected! Found {len(markets)} markets")
        
        if markets:
            # Test getting details for first market
            ticker = markets[0]['ticker']
            market = await client.get_market(ticker)
            print(f"✅ Successfully fetched market: {market.get('title', 'Unknown')}")
            
            # Test getting orderbook
            orderbook = await client.get_orderbook(ticker)
            print(f"✅ Successfully fetched orderbook for {ticker}")
            
            # Test getting trades
            trades = await client.get_trades(ticker, limit=10)
            print(f"✅ Successfully fetched {len(trades)} trades")
        
        return client
    except Exception as e:
        print(f"❌ Kalshi API connection failed: {e}")
        print("   Make sure your API key and private key are correctly set")
        return None


async def test_market_analyzer(db, client):
    """Test market analyzer functionality"""
    print("\n🔍 Testing Market Analyzer...")
    
    if not db:
        print("⚠️  Skipping analyzer test - no database connection")
        return
    
    try:
        analyzer = MarketAnalyzer(db, client)
        
        # Get a market ticker to analyze
        if client:
            markets = await client.get_markets(limit=1)
            if markets:
                ticker = markets[0]['ticker']
                
                # Test volume velocity calculation
                velocity = await analyzer.calculate_volume_velocity(ticker)
                print(f"✅ Volume velocity for {ticker}: {velocity}")
                
                # Test liquidity score
                if client:
                    liquidity = await analyzer.calculate_liquidity_score(ticker)
                    print(f"✅ Liquidity score for {ticker}: {liquidity}")
        
        print("✅ Market analyzer working!")
        
    except Exception as e:
        print(f"❌ Market analyzer test failed: {e}")


async def test_api_server():
    """Test FastAPI server endpoints"""
    print("\n🔍 Testing API Server...")
    
    try:
        import httpx
        
        # Start the server in background (you should run it separately)
        print("⚠️  Make sure the API server is running (python -m uvicorn app.main:app)")
        
        async with httpx.AsyncClient() as client:
            # Test health check
            response = await client.get("http://localhost:8000/")
            if response.status_code == 200:
                print("✅ Health check passed")
            
            # Test markets endpoint
            response = await client.get("http://localhost:8000/api/markets?limit=5")
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Markets endpoint working - {data.get('total', 0)} markets")
    
    except Exception as e:
        print(f"⚠️  API server test skipped (server not running): {e}")


async def main():
    """Run all tests"""
    print("=" * 50)
    print("🚀 Kalshi Market Tools API - Local Testing")
    print("=" * 50)
    
    # Load environment variables
    from dotenv import load_dotenv
    env_file = Path(__file__).parent / ".env"
    
    if not env_file.exists():
        print("\n⚠️  No .env file found!")
        print("   Copy .env.example to .env and fill in your credentials:")
        print("   cp .env.example .env")
        return
    
    load_dotenv(env_file)
    print(f"✅ Loaded environment from {env_file}")
    
    # Run tests
    db = await test_database_connection()
    client = await test_kalshi_api()
    
    if db:
        await test_market_analyzer(db, client)
    
    await test_api_server()
    
    # Cleanup
    if db:
        await db.close()
    if client:
        client.close()
    
    print("\n" + "=" * 50)
    print("✅ Testing complete!")
    
    if not client:
        print("\n📝 Next steps to use real data:")
        print("1. Get Kalshi API credentials from https://trading.kalshi.com/settings/api-keys")
        print("2. Add them to your .env file")
        print("3. Set up PostgreSQL database (local or Railway)")
        print("4. Run this test script again")
    
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
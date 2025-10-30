"""
Database Module
PostgreSQL database for caching market data and reducing API calls
"""

import asyncpg
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class Database:
    """PostgreSQL database for caching Kalshi market data"""
    
    def __init__(self, database_url: str):
        """
        Initialize database connection
        
        Args:
            database_url: PostgreSQL connection URL
        """
        self.database_url = database_url
        self.pool = None
    
    async def initialize(self):
        """Create connection pool and initialize tables"""
        try:
            # Create connection pool
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=2,
                max_size=10
            )
            
            logger.info("Database connection pool created")
            
            # Create tables
            await self._create_tables()
            
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
    
    async def _create_tables(self):
        """Create database tables if they don't exist"""
        async with self.pool.acquire() as conn:
            # Markets table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS markets (
                    ticker TEXT PRIMARY KEY,
                    title TEXT,
                    category TEXT,
                    status TEXT,
                    volume INTEGER,
                    yes_price REAL,
                    no_price REAL,
                    yes_bid REAL,
                    yes_ask REAL,
                    no_bid REAL,
                    no_ask REAL,
                    close_time TIMESTAMP,
                    data_json JSONB,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Trades table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT,
                    trade_id TEXT UNIQUE,
                    timestamp TIMESTAMP,
                    price REAL,
                    count INTEGER,
                    yes_price REAL,
                    no_price REAL,
                    taker_side TEXT,
                    created_time TIMESTAMP,
                    FOREIGN KEY (ticker) REFERENCES markets(ticker) ON DELETE CASCADE
                )
            """)
            
            # Create indexes for performance
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_trades_ticker 
                ON trades(ticker)
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_trades_timestamp 
                ON trades(timestamp DESC)
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_markets_category 
                ON markets(category)
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_markets_volume 
                ON markets(volume DESC)
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_markets_status 
                ON markets(status)
            """)
            
            logger.info("Database tables created/verified")
    
    async def cache_market(self, market: Dict):
        """
        Cache or update a market in the database
        
        Args:
            market: Market dictionary
        """
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO markets (
                        ticker, title, category, status, volume,
                        yes_price, no_price, yes_bid, yes_ask, no_bid, no_ask,
                        close_time, data_json, last_updated
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    ON CONFLICT (ticker) DO UPDATE SET
                        title = EXCLUDED.title,
                        category = EXCLUDED.category,
                        status = EXCLUDED.status,
                        volume = EXCLUDED.volume,
                        yes_price = EXCLUDED.yes_price,
                        no_price = EXCLUDED.no_price,
                        yes_bid = EXCLUDED.yes_bid,
                        yes_ask = EXCLUDED.yes_ask,
                        no_bid = EXCLUDED.no_bid,
                        no_ask = EXCLUDED.no_ask,
                        close_time = EXCLUDED.close_time,
                        data_json = EXCLUDED.data_json,
                        last_updated = EXCLUDED.last_updated
                """, 
                    market.get("ticker"),
                    market.get("title"),
                    market.get("category"),
                    market.get("status"),
                    market.get("volume", 0),
                    market.get("yes_price", 0),
                    market.get("no_price", 0),
                    market.get("yes_bid", 0),
                    market.get("yes_ask", 0),
                    market.get("no_bid", 0),
                    market.get("no_ask", 0),
                    market.get("close_time"),
                    json.dumps(market),
                    datetime.utcnow()
                )
        except Exception as e:
            logger.error(f"Error caching market: {e}")
    
    async def cache_trade(self, ticker: str, trade: Dict):
        """
        Cache a trade in the database
        
        Args:
            ticker: Market ticker
            trade: Trade dictionary
        """
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO trades (
                        ticker, trade_id, timestamp, price, count,
                        yes_price, no_price, taker_side, created_time
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (trade_id) DO NOTHING
                """,
                    ticker,
                    trade.get("trade_id"),
                    trade.get("timestamp"),
                    trade.get("price", 0),
                    trade.get("count", 0),
                    trade.get("yes_price", 0),
                    trade.get("no_price", 0),
                    trade.get("taker_side"),
                    trade.get("created_time")
                )
        except Exception as e:
            logger.error(f"Error caching trade: {e}")
    
    async def get_cached_markets(self, status: str = "open", limit: int = 100) -> List[Dict]:
        """
        Retrieve cached markets from database
        
        Args:
            status: Market status filter
            limit: Maximum number of markets to return
        
        Returns:
            List of market dictionaries
        """
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT data_json FROM markets
                    WHERE status = $1
                    ORDER BY volume DESC
                    LIMIT $2
                """, status, limit)
                
                markets = []
                for row in rows:
                    try:
                        market_data = row['data_json']
                        markets.append(market_data)
                    except:
                        continue
                
                return markets
        except Exception as e:
            logger.error(f"Error retrieving cached markets: {e}")
            return []
    
    async def get_market_by_ticker(self, ticker: str) -> Optional[Dict]:
        """
        Get a specific market from cache
        
        Args:
            ticker: Market ticker
        
        Returns:
            Market dictionary or None
        """
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT data_json FROM markets
                    WHERE ticker = $1
                """, ticker)
                
                if row:
                    return row['data_json']
                return None
        except Exception as e:
            logger.error(f"Error retrieving market {ticker}: {e}")
            return None
    
    async def get_cached_trades(self, ticker: str, limit: int = 100) -> List[Dict]:
        """
        Retrieve cached trades for a market
        
        Args:
            ticker: Market ticker
            limit: Maximum number of trades to return
        
        Returns:
            List of trade dictionaries
        """
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT trade_id, timestamp, price, count, yes_price,
                           no_price, taker_side, created_time
                    FROM trades
                    WHERE ticker = $1
                    ORDER BY timestamp DESC
                    LIMIT $2
                """, ticker, limit)
                
                trades = []
                for row in rows:
                    trades.append({
                        'trade_id': row['trade_id'],
                        'timestamp': row['timestamp'].isoformat() if row['timestamp'] else None,
                        'price': row['price'],
                        'count': row['count'],
                        'yes_price': row['yes_price'],
                        'no_price': row['no_price'],
                        'taker_side': row['taker_side'],
                        'created_time': row['created_time'].isoformat() if row['created_time'] else None
                    })
                
                return trades
        except Exception as e:
            logger.error(f"Error retrieving cached trades: {e}")
            return []
    
    async def search_markets(self, query: str, limit: int = 20) -> List[Dict]:
        """
        Search markets by title or ticker
        
        Args:
            query: Search query
            limit: Maximum results
        
        Returns:
            List of matching markets
        """
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT data_json FROM markets
                    WHERE ticker ILIKE $1 OR title ILIKE $1
                    ORDER BY volume DESC
                    LIMIT $2
                """, f"%{query}%", limit)
                
                markets = []
                for row in rows:
                    try:
                        markets.append(row['data_json'])
                    except:
                        continue
                
                return markets
        except Exception as e:
            logger.error(f"Error searching markets: {e}")
            return []
    
    async def clear_old_data(self, days: int = 7):
        """
        Delete old trades to manage database size
        
        Args:
            days: Delete trades older than this many days
        """
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            
            async with self.pool.acquire() as conn:
                result = await conn.execute("""
                    DELETE FROM trades
                    WHERE timestamp < $1
                """, cutoff)
                
                logger.info(f"Cleared old trades: {result}")
        except Exception as e:
            logger.error(f"Error clearing old data: {e}")
    
    async def get_statistics(self) -> Dict:
        """
        Get database statistics
        
        Returns:
            Dictionary with database stats
        """
        try:
            async with self.pool.acquire() as conn:
                # Count markets
                market_count = await conn.fetchval("SELECT COUNT(*) FROM markets")
                
                # Count trades
                trade_count = await conn.fetchval("SELECT COUNT(*) FROM trades")
                
                # Get categories
                categories = await conn.fetch("""
                    SELECT category, COUNT(*) as count
                    FROM markets
                    GROUP BY category
                    ORDER BY count DESC
                """)
                
                # Get last update time
                last_update = await conn.fetchval("""
                    SELECT MAX(last_updated) FROM markets
                """)
                
                return {
                    "market_count": market_count,
                    "trade_count": trade_count,
                    "categories": [{"category": row['category'], "count": row['count']} 
                                 for row in categories],
                    "last_update": last_update.isoformat() if last_update else None
                }
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {}
    
    async def close(self):
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")
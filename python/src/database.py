import os
import ssl
from contextlib import asynccontextmanager
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row
from src.logger import logger
from config.config import get_settings

settings = get_settings()
pool = None

async def init_db():
    global pool
    try:
        root_cert_path = os.path.join(os.path.dirname(__file__), '..', settings.DB_ROOT_CERT)
        
        # Determine SSL mode
        conn_kwargs = {
            "host": settings.DB_HOST,
            "port": settings.DB_PORT,
            "dbname": settings.DB_NAME,
            "user": settings.DB_USER,
            "password": settings.DB_PASSWORD,
            "row_factory": dict_row
        }
        
        if os.path.exists(root_cert_path):
             conn_kwargs["sslrootcert"] = root_cert_path
             conn_kwargs["sslmode"] = "verify-ca"
        else:
             logger.warning(f"Root cert not found at {root_cert_path}, defaulting to sslmode=prefer")
             conn_kwargs["sslmode"] = "prefer"

        pool = AsyncConnectionPool(
            min_size=1,
            max_size=10,
            kwargs=conn_kwargs,
            open=False
        )
        await pool.open()
        
        async with pool.connection() as conn:
             res = await conn.execute("SELECT version()")
             row = await res.fetchone()
             version = row['version'] if row else "Unknown"
        
        logger.info(f"Database connection successful", extra={'version': version, 'max_conns': 10, 'min_conns': 1})
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise e

async def close_db():
    if pool:
        await pool.close()
        logger.info("Database connection closed.")

def get_pool():
    return pool

def mask_sensitive_params(params):
    masked = []
    for p in params:
        if isinstance(p, str) and len(p) > 8:
            masked.append(p[:3] + "***")
        else:
            masked.append(p)
    return masked

async def execute_sql_with_params(sql, params=None):
    if params is None:
        params = []
    
    logger.info("Executing SQL Query", extra={'query': sql, 'params': mask_sensitive_params(params)})
    
    async with pool.connection() as conn:
        cursor = await conn.execute(sql, params)
        # For SELECT queries, return all rows
        if cursor.description:
            return await cursor.fetchall()
        return []

# Transaction Helpers
@asynccontextmanager
async def start_transaction():
    async with pool.connection() as conn:
        async with conn.transaction():
            yield conn

async def execute_sql_transaction(conn, sql, params=None):
    if params is None:
        params = []
    
    # We purposefully don't log every step inside transaction if not desired, 
    # but to match Go/JS pattern we probably should log it here too?
    # The Go implementation logs in ExecuteSQLTransaction.
    # We can rely on the caller or just execute.
    # Let's log.
    # logger.info("Executing SQL Query (Tx)", extra={'query': sql, 'params': mask_sensitive_params(params)})
    # Actually, let's keep it simple and just execute.
    
    cursor = await conn.execute(sql, params)
    if cursor.description:
        return await cursor.fetchall()
    return []

# Commit and Rollback are handled automatically by the context manager in `start_transaction`
# But if we strictly follow the manual API: Start, Commit, Rollback...
# Python's `async with conn.transaction()` is much safer.
# We will use the context manager pattern in main.py for transactions, 
# fitting the `async with` style of Python.

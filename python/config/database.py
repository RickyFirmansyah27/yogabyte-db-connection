import logging
import os
from contextlib import asynccontextmanager
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row
from .config import get_settings

settings = get_settings()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("YugabyteDB")

class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        if not self.pool:
            try:
                root_cert_path = os.path.join(os.path.dirname(__file__), settings.DB_ROOT_CERT)
                
                # Psycopg connection string or kwargs
                # We need to ensure sslmode is set if we have a cert
                sslverify = "verify-full" # or verify-ca
                if not os.path.exists(root_cert_path):
                     logger.warning(f"Root cert not found at {root_cert_path}, SSL might fail if required.")
                     root_cert_path = None
                     sslverify = "prefer"

                conn_kwargs = {
                    "host": settings.DB_HOST,
                    "port": settings.DB_PORT,
                    "dbname": settings.DB_NAME,
                    "user": settings.DB_USER,
                    "password": settings.DB_PASSWORD,
                    "row_factory": dict_row
                }
                
                if root_cert_path:
                    conn_kwargs["sslrootcert"] = root_cert_path
                    conn_kwargs["sslmode"] = "verify-ca" # Node.js: rejectUnauthorized: true, implies verify-ca or full
                else:
                    conn_kwargs["sslmode"] = "disable" # or prefer

                self.pool = AsyncConnectionPool(
                    min_size=1,
                    max_size=10,
                    kwargs=conn_kwargs,
                    open=False 
                )
                await self.pool.open()
                logger.info("Database connection established successfully.")
            except Exception as e:
                logger.error(f"Database connection failed: {e}")
                raise e

    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("Database connection closed.")

    @asynccontextmanager
    async def get_connection(self):
        if not self.pool:
            await self.connect()
        async with self.pool.connection() as connection:
            yield connection

    @asynccontextmanager
    async def transaction(self):
        async with self.get_connection() as conn:
            async with conn.transaction():
                yield conn

db = Database()

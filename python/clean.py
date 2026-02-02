import sys
import asyncio
from src.logger import logger
from src.database import init_db, close_db, execute_sql_with_params

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def main():
    try:
        await init_db()
        
        logger.info("Starting database cleanup...")
        
        await execute_sql_with_params("DROP TABLE IF EXISTS DemoAccount")
        
        logger.info("Successfully dropped table DemoAccount.")
        logger.info("Cleanup complete.")
        
        await close_db()
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

if __name__ == "__main__":
    asyncio.run(main())

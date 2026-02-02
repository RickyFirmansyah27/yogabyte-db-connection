import sys
import asyncio
from src.logger import logger
from src.database import init_db, close_db, execute_sql_with_params, start_transaction, execute_sql_transaction

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def create_database():
    await execute_sql_with_params("DROP TABLE IF EXISTS DemoAccount")
    
    await execute_sql_with_params("""
        CREATE TABLE DemoAccount (
            id int PRIMARY KEY,
            name varchar,
            age int,
            country varchar,
            balance int
        )
    """)
    
    await execute_sql_with_params("""
        INSERT INTO DemoAccount VALUES
            (1, 'Jessica', 28, 'USA', 10000),
            (2, 'John', 28, 'Canada', 9000)
    """)
    
    logger.info("Successfully created table DemoAccount.")

async def select_accounts():
    logger.info("Selecting accounts:")
    rows = await execute_sql_with_params("SELECT name, age, country, balance FROM DemoAccount")
    for row in rows:
        logger.info("Account", extra={
            'account_name': row['name'], 
            'age': row['age'], 
            'country': row['country'], 
            'balance': row['balance']
        })

async def transfer_money_between_accounts(amount: int):
    try:
        async with start_transaction() as conn:
            await execute_sql_transaction(
                conn,
                "UPDATE DemoAccount SET balance = balance - %s WHERE name = %s",
                [amount, 'Jessica']
            )
            await execute_sql_transaction(
                conn,
                "UPDATE DemoAccount SET balance = balance + %s WHERE name = %s",
                [amount, 'John']
            )
        
        logger.info(f"Transferred {amount} between accounts.")
    except Exception as e:
        logger.error(f"Transaction failed: {e}")

async def main():
    try:
        await init_db()
        
        await create_database()
        await select_accounts()
        await transfer_money_between_accounts(800)
        await select_accounts()
        
        await close_db()
    except Exception as e:
        logger.error(f"Execution error: {e}")

if __name__ == "__main__":
    asyncio.run(main())

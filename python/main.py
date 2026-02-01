import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from config.database import db


async def create_database():
    async with db.transaction() as conn:
        await conn.execute("DROP TABLE IF EXISTS DemoAccount")
        await conn.execute("""
            CREATE TABLE DemoAccount (
                id int PRIMARY KEY,
                name varchar,
                age int,
                country varchar,
                balance int
            )
        """)
        await conn.execute("""
            INSERT INTO DemoAccount VALUES
                (1, 'Jessica', 28, 'USA', 10000),
                (2, 'John', 28, 'Canada', 9000)
        """)
    print(">>>> Successfully created table DemoAccount.")


async def select_accounts():
    print(">>>> Selecting accounts:")
    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT name, age, country, balance FROM DemoAccount")
        rows = await cursor.fetchall()
        for row in rows:
            print(f"name = {row['name']}, age = {row['age']}, country = {row['country']}, balance = {row['balance']}")


async def transfer_money_between_accounts(amount: int):
    async with db.transaction() as conn:
        await conn.execute(
            "UPDATE DemoAccount SET balance = balance - %s WHERE name = %s",
            [amount, 'Jessica']
        )
        await conn.execute(
            "UPDATE DemoAccount SET balance = balance + %s WHERE name = %s",
            [amount, 'John']
        )
    print(f">>>> Transferred {amount} between accounts.")


async def main():
    await db.connect()
    print(">>>> Successfully connected to YugabyteDB!")
    
    await create_database()
    await select_accounts()
    await transfer_money_between_accounts(800)
    await select_accounts()
    
    await db.close()
    print(">>>> Database connection closed.")


if __name__ == "__main__":
    asyncio.run(main())

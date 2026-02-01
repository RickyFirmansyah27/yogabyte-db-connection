import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException

if sys.platform == "win32":
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from config.database import db

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    print(">>>> Successfully connected to YugabyteDB!")
    yield
    await db.close()
    print(">>>> Database connection closed.")

app = FastAPI(title="YugabyteDB Demo", lifespan=lifespan)



@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.post("/setup")
async def setup_database():
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
    return {"message": "Setup completed"}


@app.get("/accounts")
async def get_accounts():
    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT * FROM DemoAccount")
        return await cursor.fetchall()


@app.post("/transfer")
async def transfer(from_name: str, to_name: str, amount: int):
    async with db.transaction() as conn:
        await conn.execute(
            "UPDATE DemoAccount SET balance = balance - %s WHERE name = %s",
            [amount, from_name]
        )
        await conn.execute(
            "UPDATE DemoAccount SET balance = balance + %s WHERE name = %s",
            [amount, to_name]
        )
    return {"message": f"Transferred {amount} from {from_name} to {to_name}"}



if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

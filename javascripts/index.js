import { InitLogger, GetLogger, Logger } from './src/logger.js';
import { Connect, CloseDB, ExecuteSQLWithParams, StartTransaction, CommitTransaction, RollbackTransaction, ExecuteSQLTransaction } from './src/database.js';

const createDatabase = async () => {
  await ExecuteSQLWithParams("DROP TABLE IF EXISTS DemoAccount");

  await ExecuteSQLWithParams(`CREATE TABLE DemoAccount (
            id int PRIMARY KEY,
            name varchar,
            age int,
            country varchar,
            balance int)`);

  await ExecuteSQLWithParams(`INSERT INTO DemoAccount VALUES
            (1, 'Jessica', 28, 'USA', 10000),
            (2, 'John', 28, 'Canada', 9000)`);

  Logger.info("Successfully created table DemoAccount.");
};

const selectAccounts = async () => {
  Logger.info("Selecting accounts:");
  const result = await ExecuteSQLWithParams("SELECT name, age, country, balance FROM DemoAccount");

  for (const row of result.rows) {
    Logger.info("Account", {
      name: row.name,
      age: row.age,
      country: row.country,
      balance: row.balance
    });
  }
};

const transferMoneyBetweenAccounts = async (amount) => {
  let client;
  try {
    client = await StartTransaction();

    await ExecuteSQLTransaction(
      client,
      "UPDATE DemoAccount SET balance = balance - $1 WHERE name = 'Jessica'",
      [amount]
    );
    await ExecuteSQLTransaction(
      client,
      "UPDATE DemoAccount SET balance = balance + $1 WHERE name = 'John'",
      [amount]
    );

    await CommitTransaction(client);

    Logger.info(`Transferred ${amount} between accounts.`);
  } catch (err) {
    if (client) {
      await RollbackTransaction(client);
    }
    Logger.error("Transaction failed", { error: err.message });
  }
};

const main = async () => {
  try {
    InitLogger();
    await Connect();

    await createDatabase();
    await selectAccounts();
    await transferMoneyBetweenAccounts(800);
    await selectAccounts();

    await CloseDB();
  } catch (err) {
    console.error("Error during execution:", err);
  }
};

await main();

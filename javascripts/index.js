import {
  DBConnection,
  commandWithParams,
  startTransaction,
  executeSQLTransaction,
  commitTransaction,
  rollbackTransaction
} from './src/database.js';

const createDatabase = async () => {
  let stmt = "DROP TABLE IF EXISTS DemoAccount";
  await commandWithParams(stmt);

  stmt = `CREATE TABLE DemoAccount (
            id int PRIMARY KEY,
            name varchar,
            age int,
            country varchar,
            balance int)`;
  await commandWithParams(stmt);

  stmt = `INSERT INTO DemoAccount VALUES
            (1, 'Jessica', 28, 'USA', 10000),
            (2, 'John', 28, 'Canada', 9000)`;
  await commandWithParams(stmt);

  console.log(">>>> Successfully created table DemoAccount.");
};

const selectAccounts = async () => {
  console.log(">>>> Selecting accounts:");
  const rows = await commandWithParams(
    "SELECT name, age, country, balance FROM DemoAccount"
  );

  for (const row of rows) {
    console.log(
      "name = %s, age = %d, country = %s, balance = %d",
      row.name,
      row.age,
      row.country,
      row.balance
    );
  }
};

const transferMoneyBetweenAccounts = async (amount) => {
  let client;
  try {
    client = await startTransaction();

    await executeSQLTransaction(
      client,
      "UPDATE DemoAccount SET balance = balance - $1 WHERE name = 'Jessica'",
      [amount]
    );
    await executeSQLTransaction(
      client,
      "UPDATE DemoAccount SET balance = balance + $1 WHERE name = 'John'",
      [amount]
    );

    await commitTransaction(client);

    console.log(">>>> Transferred %d between accounts.", amount);
  } catch (err) {
    if (client) {
      await rollbackTransaction(client);
    }
    if (err.code == 40001) {
      console.error(
        `The operation is aborted due to a concurrent transaction that is modifying the same set of rows. Consider adding retry logic or using the pessimistic locking.`
      );
    }
    throw err;
  }
};

const main = async () => {
  try {
    await DBConnection();
    await createDatabase();
    await selectAccounts();
    await transferMoneyBetweenAccounts(800);
    await selectAccounts();
  } catch (err) {
    console.error("Error during execution:", err);
  }
  // Pool keeps the process alive, allow explicit exit or let it run if it was a server
  process.exit(0);
};

await main();

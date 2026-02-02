package main

import (
	"context"
	"fmt"
	"log/slog"

	"yogabyte-db-connection/src/database"
	"yogabyte-db-connection/src/logger"
)

func main() {
	// Initialize Logger
	logger.InitLogger()

	// Initialize Database
	if err := database.Connect(); err != nil {
		slog.Error("Failed to initialize database", "error", err)
		panic(err)
	}
	defer database.CloseDB()

	// Operations
	createDatabase()
	selectAccounts()
	transferMoneyBetweenAccounts(800)
	selectAccounts()
}

func createDatabase() {
	rows, err := database.ExecuteSQLWithParams(`DROP TABLE IF EXISTS DemoAccount`)
	checkIfError(err)
	rows.Close()

	rows, err = database.ExecuteSQLWithParams(`CREATE TABLE DemoAccount (
		id int PRIMARY KEY,
		name varchar,
		age int,
		country varchar,
		balance int)`)
	checkIfError(err)
	rows.Close()

	rows, err = database.ExecuteSQLWithParams(`INSERT INTO DemoAccount VALUES
		(1, 'Jessica', 28, 'USA', 10000),
		(2, 'John', 28, 'Canada', 9000)`)
	checkIfError(err)
	rows.Close()

	slog.Info("Successfully created table DemoAccount.")
}

func selectAccounts() {
	slog.Info("Selecting accounts:")

	rows, err := database.ExecuteSQLWithParams("SELECT name, age, country, balance FROM DemoAccount")
	checkIfError(err)
	defer rows.Close()

	for rows.Next() {
		var name, country string
		var age, balance int
		err = rows.Scan(&name, &age, &country, &balance)
		checkIfError(err)
		slog.Info("Account", "name", name, "age", age, "country", country, "balance", balance)
	}
}

func transferMoneyBetweenAccounts(amount int) {
	ctx := context.Background()
	tx, err := database.StartTransaction(ctx)
	checkIfError(err)
	defer database.RollbackTransaction(ctx, tx) // Safe to call even if committed (will fail harmlessly or be ignored if committed)

	// Transaction Step 1
	rows1, err := database.ExecuteSQLTransaction(ctx, tx, `UPDATE DemoAccount SET balance = balance - $1 WHERE name = 'Jessica'`, amount)
	if checkIfTxError(err) {
		return
	}
	rows1.Close()

	// Transaction Step 2
	rows2, err := database.ExecuteSQLTransaction(ctx, tx, `UPDATE DemoAccount SET balance = balance + $1 WHERE name = 'John'`, amount)
	if checkIfTxError(err) {
		return
	}
	rows2.Close()

	err = database.CommitTransaction(ctx, tx)
	if checkIfTxError(err) {
		return
	}

	slog.Info(fmt.Sprintf("Transferred %d between accounts.", amount))
}

func checkIfError(err error) {
	if err != nil {
		slog.Error("Operation failed", "error", err)
		panic(err)
	}
}

func checkIfTxError(err error) bool {
	if err != nil {
		slog.Error("Transaction failed", "error", err)
		return true
	}
	return false
}

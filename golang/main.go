package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

var (
	host        string
	port        string
	dbName      string
	dbUser      string
	dbPassword  string
	sslRootCert string
)

func loadConfig() {
	_, currentFile, _, _ := runtime.Caller(0)
	dir := filepath.Dir(currentFile)
	envPath := filepath.Join(dir, ".env")

	if err := godotenv.Load(envPath); err != nil {
		fmt.Printf("Error loading .env file: %v\n", err)
	}

	host = os.Getenv("DB_HOST")
	port = os.Getenv("DB_PORT")
	dbName = os.Getenv("DB_NAME")
	dbUser = os.Getenv("DB_USER")
	dbPassword = os.Getenv("DB_PASSWORD")
	sslRootCert = filepath.Join(dir, "config", "root.crt")
}

func main() {
	loadConfig()

	// Build connection string like Python does
	connString := fmt.Sprintf(
		"host=%s port=%s dbname=%s user=%s password=%s sslmode=verify-ca sslrootcert=%s",
		host, port, dbName, dbUser, dbPassword, sslRootCert,
	)

	fmt.Printf(">>>> Connecting to: host=%s port=%s dbname=%s\n", host, port, dbName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connString)
	checkIfError(err)
	defer conn.Close(context.Background())

	fmt.Println(">>>> Successfully connected to YugabyteDB!")

	createDatabase(ctx, conn)
	selectAccounts(ctx, conn)
	transferMoneyBetweenAccounts(ctx, conn, 800)
	selectAccounts(ctx, conn)
}

func createDatabase(ctx context.Context, conn *pgx.Conn) {
	_, err := conn.Exec(ctx, `DROP TABLE IF EXISTS DemoAccount`)
	checkIfError(err)

	_, err = conn.Exec(ctx, `CREATE TABLE DemoAccount (
		id int PRIMARY KEY,
		name varchar,
		age int,
		country varchar,
		balance int)`)
	checkIfError(err)

	_, err = conn.Exec(ctx, `INSERT INTO DemoAccount VALUES
		(1, 'Jessica', 28, 'USA', 10000),
		(2, 'John', 28, 'Canada', 9000)`)
	checkIfError(err)

	fmt.Println(">>>> Successfully created table DemoAccount.")
}

func selectAccounts(ctx context.Context, conn *pgx.Conn) {
	fmt.Println(">>>> Selecting accounts:")

	rows, err := conn.Query(ctx, "SELECT name, age, country, balance FROM DemoAccount")
	checkIfError(err)
	defer rows.Close()

	for rows.Next() {
		var name, country string
		var age, balance int
		err = rows.Scan(&name, &age, &country, &balance)
		checkIfError(err)
		fmt.Printf("name = %s, age = %v, country = %s, balance = %v\n",
			name, age, country, balance)
	}
}

func transferMoneyBetweenAccounts(ctx context.Context, conn *pgx.Conn, amount int) {
	tx, err := conn.Begin(ctx)
	checkIfError(err)

	_, err = tx.Exec(ctx, `UPDATE DemoAccount SET balance = balance - $1 WHERE name = 'Jessica'`, amount)
	if checkIfTxAborted(err) {
		tx.Rollback(ctx)
		return
	}

	_, err = tx.Exec(ctx, `UPDATE DemoAccount SET balance = balance + $1 WHERE name = 'John'`, amount)
	if checkIfTxAborted(err) {
		tx.Rollback(ctx)
		return
	}

	err = tx.Commit(ctx)
	if checkIfTxAborted(err) {
		return
	}

	fmt.Printf(">>>> Transferred %d between accounts.\n", amount)
}

func checkIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func checkIfTxAborted(err error) bool {
	if err != nil {
		fmt.Printf("Transaction aborted: %v\n", err)
		return true
	}
	return false
}

package database

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

var dbPool *pgxpool.Pool

func Connect() error {
	// Initialize config explicitly to find .env properly
	// _, currentFile, _, _ := runtime.Caller(0)
	// dir := filepath.Dir(currentFile)
	// Current file is in src/database, .env is in project root (../../.env from here, or just find it relative to cwd if running from root)
	// The original main.go found it in project root. Let's try loading from CWD first, then fallback.

	// Try loading .env from CWD (usually project root)
	if err := godotenv.Load(); err != nil {
		// If running from src/database specific context, might fail, but standard is root
		slog.Warn("Error loading .env file (might be missing or checking parent dirs)", "error", err)
	}

	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	dbName := os.Getenv("DB_NAME")
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")

	// Construct connection string logic again to support the existing .env structure
	// We need the root.crt path. Assuming running from root, config/root.crt
	rootCertPath, _ := filepath.Abs("config/root.crt")

	// Create connection string
	connStr := fmt.Sprintf(
		"host=%s port=%s dbname=%s user=%s password=%s sslmode=verify-ca sslrootcert=%s",
		host, port, dbName, dbUser, dbPassword, rootCertPath,
	)

	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return fmt.Errorf("unable to parse connection string: %v", err)
	}

	// Production connection pool settings
	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute
	config.HealthCheckPeriod = 1 * time.Minute
	config.ConnConfig.ConnectTimeout = 10 * time.Second

	dbPool, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %v", err)
	}

	// Test connection
	var version string
	err = dbPool.QueryRow(context.Background(), "SELECT version()").Scan(&version)
	if err != nil {
		dbPool.Close() // Close if initial test fails
		return fmt.Errorf("database connection test failed: %v", err)
	}

	slog.Info("Database connection successful", "component", "Neon DB", "version", version,
		"max_conns", config.MaxConns, "min_conns", config.MinConns)
	return nil
}

func GetDBPool() *pgxpool.Pool {
	return dbPool
}

func DBConnection() error {
	if dbPool == nil {
		return fmt.Errorf("database not initialized")
	}
	return nil
}

func maskSensitiveParams(params []interface{}) []interface{} {
	masked := make([]interface{}, len(params))
	for i, p := range params {
		if str, ok := p.(string); ok && len(str) > 8 {
			masked[i] = str[:3] + "***"
		} else {
			masked[i] = p
		}
	}
	return masked
}

func ExecuteSQLWithParams(sql string, params ...interface{}) (pgx.Rows, error) {
	slog.Info("Executing SQL Query", "component", "Neon DB", "query", sql, "params", maskSensitiveParams(params))

	rows, err := dbPool.Query(context.Background(), sql, params...)
	if err != nil {
		slog.Error("Query Failed", "component", "Neon DB", "error", err)
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	return rows, nil
}

func ExecuteQueryRowWithParams(sql string, params ...interface{}) pgx.Row {
	slog.Info("Executing Single Row Query", "component", "Neon DB", "query", sql, "params", maskSensitiveParams(params))
	return dbPool.QueryRow(context.Background(), sql, params...)
}

func StartTransaction(ctx context.Context) (pgx.Tx, error) {
	tx, err := dbPool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %v", err)
	}

	return tx, nil
}

func ExecuteSQLTransaction(ctx context.Context, tx pgx.Tx, sql string, params ...interface{}) (pgx.Rows, error) {
	rows, err := tx.Query(ctx, sql, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query in transaction: %v", err)
	}

	return rows, nil
}

func RollbackTransaction(ctx context.Context, tx pgx.Tx) error {
	err := tx.Rollback(ctx)
	if err != nil {
		return fmt.Errorf("failed to rollback transaction: %v", err)
	}
	return nil
}

func CommitTransaction(ctx context.Context, tx pgx.Tx) error {
	err := tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	return nil
}

func CloseDB() {
	if dbPool != nil {
		dbPool.Close()
	}
}

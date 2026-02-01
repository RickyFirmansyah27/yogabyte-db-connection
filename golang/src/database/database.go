package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"myapp/config"
	"myapp/src/logger"
)

var (
	pool *pgxpool.Pool
	log  = logger.New()
)

func InitDB() error {
	cfg, err := config.LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	poolConfig, err := cfg.GetPoolConfig()
	if err != nil {
		return fmt.Errorf("failed to get pool config: %w", err)
	}

	pool, err = pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}

	return nil
}

func GetPool() *pgxpool.Pool {
	return pool
}

func CloseDB() {
	if pool != nil {
		pool.Close()
	}
}

func TestConnection(ctx context.Context) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Error("Database connection error", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}
	defer conn.Release()

	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		log.Error("Database query error", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	var version string
	err = conn.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		log.Error("Database version query error", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	log.Info("Database connection successfully", map[string]interface{}{
		"connection": result == 1,
	})
	log.Info(fmt.Sprintf("version: %s", version))

	return nil
}

func CommandWithParams(ctx context.Context, sql string, params ...interface{}) (pgx.Rows, error) {
	log.Info(fmt.Sprintf("SQL: %s - Params: %v", sql, params))

	rows, err := pool.Query(ctx, sql, params...)
	if err != nil {
		log.Error("Database query error", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	return rows, nil
}

func ExecuteCommand(ctx context.Context, sql string, params ...interface{}) error {
	log.Info(fmt.Sprintf("SQL: %s - Params: %v", sql, params))

	_, err := pool.Exec(ctx, sql, params...)
	if err != nil {
		log.Error("Database execute error", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	return nil
}

type Transaction struct {
	tx pgx.Tx
}

func StartTransaction(ctx context.Context) (*Transaction, error) {
	log.Info("Starting transaction")

	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Error("Failed to start transaction", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	return &Transaction{tx: tx}, nil
}

func (t *Transaction) Execute(ctx context.Context, sql string, params ...interface{}) error {
	log.Info(fmt.Sprintf("Transaction SQL: %s - Params: %v", sql, params))

	_, err := t.tx.Exec(ctx, sql, params...)
	if err != nil {
		log.Error("Transaction execute error", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	return nil
}

func (t *Transaction) Query(ctx context.Context, sql string, params ...interface{}) (pgx.Rows, error) {
	log.Info(fmt.Sprintf("Transaction SQL: %s - Params: %v", sql, params))

	rows, err := t.tx.Query(ctx, sql, params...)
	if err != nil {
		log.Error("Transaction query error", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	return rows, nil
}

func (t *Transaction) Commit(ctx context.Context) error {
	log.Info("Committing transaction")
	return t.tx.Commit(ctx)
}

func (t *Transaction) Rollback(ctx context.Context) error {
	log.Info("Rolling back transaction")
	return t.tx.Rollback(ctx)
}

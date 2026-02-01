package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"yogabyte-db-connection/config"
	"yogabyte-db-connection/src/logger"
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

	log.Info(fmt.Sprintf("Connecting to: host=%s port=%s dbname=%s", cfg.Host, cfg.Port, cfg.Database))

	poolConfig, err := cfg.GetPoolConfig()
	if err != nil {
		return fmt.Errorf("failed to get pool config: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Error("Database connection error", map[string]interface{}{
			"error": err.Error(),
		})
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

func TestConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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

	log.Info("Successfully connected to YugabyteDB!")
	log.Info(fmt.Sprintf("version: %s", version))

	return nil
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

func Query(ctx context.Context, sql string, params ...interface{}) (*pgxpool.Pool, error) {
	log.Info(fmt.Sprintf("SQL: %s - Params: %v", sql, params))
	return pool, nil
}

type Transaction struct {
	tx interface {
		Exec(ctx context.Context, sql string, arguments ...interface{}) (interface{}, error)
		Commit(ctx context.Context) error
		Rollback(ctx context.Context) error
	}
}

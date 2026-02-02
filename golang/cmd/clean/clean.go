package main

import (
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

	slog.Info("Starting database cleanup...")

	// Execute Drop
	rows, err := database.ExecuteSQLWithParams(`DROP TABLE IF EXISTS DemoAccount`)
	if err != nil {
		slog.Error("Failed to drop table", "error", err)
		panic(err)
	}
	rows.Close() // Ensure connection is released

	slog.Info("Successfully dropped table DemoAccount.")
	slog.Info("Cleanup complete.")
}

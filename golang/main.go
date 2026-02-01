package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"

	"myapp/src/database"
	appLogger "myapp/src/logger"
)

type Account struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Age     int    `json:"age"`
	Country string `json:"country"`
	Balance int    `json:"balance"`
}

type TransferRequest struct {
	Amount int `json:"amount"`
}

var appLog = appLogger.New()

func main() {
	ctx := context.Background()

	if err := database.InitDB(); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer database.CloseDB()

	if err := database.TestConnection(ctx); err != nil {
		log.Fatalf("Failed to test database connection: %v", err)
	}

	app := fiber.New(fiber.Config{
		AppName: "YugabyteDB Fiber API",
	})

	app.Use(recover.New())
	app.Use(logger.New())
	app.Use(cors.New())

	app.Get("/", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"message": "YugabyteDB Fiber API",
			"status":  "running",
		})
	})

	api := app.Group("/api")

	api.Post("/setup", setupDatabase)
	api.Get("/accounts", getAccounts)
	api.Post("/transfer", transferMoney)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := app.Listen(":3000"); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	appLog.Info("Server started on http://localhost:3000")

	<-quit
	appLog.Info("Shutting down server...")
	if err := app.Shutdown(); err != nil {
		log.Fatalf("Failed to shutdown server: %v", err)
	}
}

func setupDatabase(c *fiber.Ctx) error {
	ctx := c.Context()

	if err := database.ExecuteCommand(ctx, "DROP TABLE IF EXISTS DemoAccount"); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	createTableSQL := `CREATE TABLE DemoAccount (
		id int PRIMARY KEY,
		name varchar,
		age int,
		country varchar,
		balance int
	)`
	if err := database.ExecuteCommand(ctx, createTableSQL); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	insertSQL := `INSERT INTO DemoAccount VALUES
		(1, 'Jessica', 28, 'USA', 10000),
		(2, 'John', 28, 'Canada', 9000)`
	if err := database.ExecuteCommand(ctx, insertSQL); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	appLog.Info("Successfully created table DemoAccount")

	return c.JSON(fiber.Map{
		"message": "Database setup completed successfully",
	})
}

func getAccounts(c *fiber.Ctx) error {
	ctx := c.Context()

	rows, err := database.CommandWithParams(ctx, "SELECT id, name, age, country, balance FROM DemoAccount")
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	defer rows.Close()

	var accounts []Account
	for rows.Next() {
		var acc Account
		if err := rows.Scan(&acc.ID, &acc.Name, &acc.Age, &acc.Country, &acc.Balance); err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
		accounts = append(accounts, acc)
	}

	if err := rows.Err(); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	appLog.Info("Selecting accounts")
	for _, acc := range accounts {
		appLog.Info(fmt.Sprintf("name = %s, age = %d, country = %s, balance = %d",
			acc.Name, acc.Age, acc.Country, acc.Balance))
	}

	return c.JSON(fiber.Map{
		"accounts": accounts,
	})
}

func transferMoney(c *fiber.Ctx) error {
	ctx := c.Context()

	var req TransferRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	if req.Amount <= 0 {
		return c.Status(400).JSON(fiber.Map{
			"error": "Amount must be greater than 0",
		})
	}

	tx, err := database.StartTransaction(ctx)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	if err := tx.Execute(ctx, "UPDATE DemoAccount SET balance = balance - $1 WHERE name = 'Jessica'", req.Amount); err != nil {
		tx.Rollback(ctx)
		if pgErr, ok := err.(interface{ Code() string }); ok && pgErr.Code() == "40001" {
			return c.Status(409).JSON(fiber.Map{
				"error": "The operation is aborted due to a concurrent transaction. Consider adding retry logic.",
			})
		}
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	if err := tx.Execute(ctx, "UPDATE DemoAccount SET balance = balance + $1 WHERE name = 'John'", req.Amount); err != nil {
		tx.Rollback(ctx)
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	if err := tx.Commit(ctx); err != nil {
		tx.Rollback(ctx)
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	appLog.Info(fmt.Sprintf("Transferred %d between accounts", req.Amount))

	return c.JSON(fiber.Map{
		"message": fmt.Sprintf("Successfully transferred %d from Jessica to John", req.Amount),
	})
}

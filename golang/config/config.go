package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type DBConfig struct {
	Host        string
	Port        string
	Database    string
	User        string
	Password    string
	SSLRootCert string
}

func LoadConfig() (*DBConfig, error) {
	_, currentFile, _, _ := runtime.Caller(0)
	configDir := filepath.Dir(currentFile)
	envPath := filepath.Join(configDir, "..", ".env")

	if err := godotenv.Load(envPath); err != nil {
		return nil, fmt.Errorf("error loading .env file: %w", err)
	}

	certPath := filepath.Join(configDir, "root.crt")

	return &DBConfig{
		Host:        os.Getenv("DB_HOST"),
		Port:        os.Getenv("DB_PORT"),
		Database:    os.Getenv("DB_NAME"),
		User:        os.Getenv("DB_USER"),
		Password:    os.Getenv("DB_PASSWORD"),
		SSLRootCert: certPath,
	}, nil
}

// ResolveIPv4 resolves hostname to IPv4 address
func (c *DBConfig) ResolveIPv4() (string, error) {
	ips, err := net.LookupIP(c.Host)
	if err != nil {
		return "", fmt.Errorf("failed to resolve host: %w", err)
	}

	for _, ip := range ips {
		if ipv4 := ip.To4(); ipv4 != nil {
			return ipv4.String(), nil
		}
	}

	return "", fmt.Errorf("no IPv4 address found for host: %s", c.Host)
}

func (c *DBConfig) GetPoolConfig() (*pgxpool.Config, error) {
	// Resolve to IPv4 to avoid IPv6 issues
	ipv4, err := c.ResolveIPv4()
	if err != nil {
		return nil, err
	}

	port, err := strconv.Atoi(c.Port)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %w", err)
	}

	// Read SSL certificate
	caCert, err := os.ReadFile(c.SSLRootCert)
	if err != nil {
		return nil, fmt.Errorf("failed to read root.crt: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append CA certificate")
	}

	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: false,
		ServerName:         c.Host, // Use original hostname for TLS verification
		MinVersion:         tls.VersionTLS12,
	}

	connConfig, err := pgx.ParseConfig("")
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	connConfig.Host = ipv4 // Use resolved IPv4
	connConfig.Port = uint16(port)
	connConfig.Database = c.Database
	connConfig.User = c.User
	connConfig.Password = c.Password
	connConfig.TLSConfig = tlsConfig
	connConfig.ConnectTimeout = 30_000_000_000 // 30 seconds in nanoseconds

	poolConfig, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, fmt.Errorf("failed to parse pool config: %w", err)
	}

	poolConfig.ConnConfig = connConfig
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 1

	return poolConfig, nil
}

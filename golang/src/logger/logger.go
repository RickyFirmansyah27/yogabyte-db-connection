package logger

import (
	"encoding/json"
	"fmt"
	"time"
)

const contextLogger = "[Yugabyte DB - connection]"

type Logger struct{}

func New() *Logger {
	return &Logger{}
}

func (l *Logger) Info(message string, meta ...map[string]interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	metaStr := ""
	if len(meta) > 0 && len(meta[0]) > 0 {
		if b, err := json.Marshal(meta[0]); err == nil {
			metaStr = " " + string(b)
		}
	}
	fmt.Printf("[%s] [INFO] %s | %s%s\n", timestamp, contextLogger, message, metaStr)
}

func (l *Logger) Error(message string, meta ...map[string]interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	metaStr := ""
	if len(meta) > 0 && len(meta[0]) > 0 {
		if b, err := json.Marshal(meta[0]); err == nil {
			metaStr = " " + string(b)
		}
	}
	fmt.Printf("[%s] [ERROR] %s | %s%s\n", timestamp, contextLogger, message, metaStr)
}

func (l *Logger) Warn(message string, meta ...map[string]interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	metaStr := ""
	if len(meta) > 0 && len(meta[0]) > 0 {
		if b, err := json.Marshal(meta[0]); err == nil {
			metaStr = " " + string(b)
		}
	}
	fmt.Printf("[%s] [WARN] %s | %s%s\n", timestamp, contextLogger, message, metaStr)
}

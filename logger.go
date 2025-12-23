package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// Logger handles file-based logging
type Logger struct {
	logger *log.Logger
	mu     sync.Mutex
}

var globalLogger *Logger

// InitializeLogger sets up the global logger with file output
func InitializeLogger(nodeName string) error {
	// Create log directory if it doesn't exist
	logDir := "./log"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %v", err)
	}

	// Create single log file for this node
	logFile := filepath.Join(logDir, fmt.Sprintf("%s.log", nodeName))

	// Open log file and truncate on each server start so logs start fresh
	logF, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}

	// Create logger with timestamps
	globalLogger = &Logger{
		logger: log.New(logF, "", log.LstdFlags),
	}

	return nil
}

// LogInfo writes info-level messages to the log file
// enable: true to log, false to skip
func LogInfo(enable bool, format string, args ...interface{}) {
	if enable && globalLogger != nil {
		globalLogger.mu.Lock()
		defer globalLogger.mu.Unlock()
		globalLogger.logger.Printf("[INFO] "+format, args...)
	}
}

// LogError writes error-level messages to the log file
// enable: true to log, false to skip
func LogError(enable bool, format string, args ...interface{}) {
	if enable && globalLogger != nil {
		globalLogger.mu.Lock()
		defer globalLogger.mu.Unlock()
		globalLogger.logger.Printf("[ERROR] "+format, args...)
	}
}

// LogPrintln is a direct replacement for fmt.Println that writes to log file
func LogPrintln(args ...interface{}) {
	if globalLogger != nil {
		globalLogger.mu.Lock()
		defer globalLogger.mu.Unlock()
		globalLogger.logger.Println(args...)
	}
}

// Console logging functions - these write to terminal/console instead of log files

// ConsolePrintf writes formatted output to console (terminal)
func ConsolePrintf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	LogInfo(true, format, args...)
}

// ConsolePrintln writes output to console (terminal)
func ConsolePrintln(args ...interface{}) {
	fmt.Println(args...)
	LogInfo(true, fmt.Sprintln(args...))
}

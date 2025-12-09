package logger

import (
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/xrpscan/platform/config"
	"gopkg.in/natefinch/lumberjack.v2"
)

// flushWriter wraps a Writer and ensures Flush is called after each Write
type flushWriter struct {
	io.Writer
}

func (fw *flushWriter) Write(p []byte) (n int, err error) {
	n, err = fw.Writer.Write(p)
	if f, ok := fw.Writer.(interface{ Sync() error }); ok {
		f.Sync() // Flush immediately
	}
	return n, err
}

var Log zerolog.Logger
var loggerOnce sync.Once

func New() {
	loggerOnce.Do(func() {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		level, err := zerolog.ParseLevel(config.EnvLogLevel())
		if err == nil {
			zerolog.SetGlobalLevel(level)
		}

		// In indexer mode, write logs to stderr to avoid interfering with IPC on stdout
		// Check if we're in indexer mode by checking for IPC-related environment or flag
		var logOutput io.Writer = os.Stdout
		if os.Getenv("INDEXER_MODE") == "true" || os.Getenv("MODE") == "indexer" {
			logOutput = os.Stderr
			// Wrap stderr to ensure immediate flushing in indexer mode
			logOutput = &flushWriter{Writer: logOutput}
		}

		// Create console writer with no buffering for indexer mode
		consoleWriter := zerolog.ConsoleWriter{
			Out:        logOutput,
			TimeFormat: time.RFC3339,
			NoColor:    true,
		}

		// Create multi-writer for both console and file
		var writers []io.Writer
		writers = append(writers, consoleWriter)

		// Add file writer if enabled
		if config.EnvLogFileEnabled() {
			logFilePath := config.EnvLogFilePath()
			if logFilePath == "" {
				logFilePath = "logs/platform.log"
			}

			// Ensure log directory exists
			logDir := filepath.Dir(logFilePath)
			if err := os.MkdirAll(logDir, 0755); err != nil {
				// If we can't create directory, just log to console
				tempLogger := zerolog.New(consoleWriter).With().Timestamp().Logger()
				tempLogger.Error().
					Err(err).Str("log_dir", logDir).Msg("Failed to create log directory, logging to console only")
			} else {
				// Create file writer with rotation
				fileWriter := &lumberjack.Logger{
					Filename:   logFilePath,
					MaxSize:    config.EnvLogFileMaxSize(), // MB
					MaxBackups: config.EnvLogFileMaxBackups(),
					MaxAge:     config.EnvLogFileMaxAge(), // days
					Compress:   true,
				}
				writers = append(writers, fileWriter)

				// Log that file logging is enabled
				tempLogger := zerolog.New(consoleWriter).With().Timestamp().Logger()
				tempLogger.Info().
					Str("log_file", logFilePath).
					Int("max_size_mb", config.EnvLogFileMaxSize()).
					Int("max_backups", config.EnvLogFileMaxBackups()).
					Int("max_age_days", config.EnvLogFileMaxAge()).
					Msg("File logging enabled")
			}
		}

		// Create multi-writer
		multiWriter := io.MultiWriter(writers...)
		Log = zerolog.New(multiWriter).With().Timestamp().Logger()
	})
}

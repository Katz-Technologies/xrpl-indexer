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

var Log zerolog.Logger
var loggerOnce sync.Once

func New() {
	loggerOnce.Do(func() {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		level, err := zerolog.ParseLevel(config.EnvLogLevel())
		if err == nil {
			zerolog.SetGlobalLevel(level)
		}

		// Create console writer for stdout
		consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}

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

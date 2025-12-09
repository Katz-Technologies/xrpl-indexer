package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/joho/godotenv"
)

func EnvLoad(filenames ...string) {
	if len(filenames) == 0 {
		filenames = append(filenames, ".env")
	}
	for _, filename := range filenames {
		log.Printf("Loading configuration file: %s", filename)
		err := godotenv.Load(filename)
		if err != nil {
			log.Printf("Error loading configuration file %s: %v", filename, err)
			// Don't fatal - allow process to continue with environment variables
			// that might be set directly
		} else {
			log.Printf("Successfully loaded configuration file: %s", filename)
		}
	}
}

/*
* Service settings
 */

// Get HTTP server hostname
func EnvServerHost() string {
	return os.Getenv("SERVER_HOST")
}

// Get HTTP server port
func EnvServerPort() string {
	return os.Getenv("SERVER_PORT")
}

// Get default log level
func EnvLogLevel() string {
	return os.Getenv("LOG_LEVEL")
}

// Get default log type
func EnvLogType() string {
	return os.Getenv("LOG_TYPE")
}

// Get log file path
func EnvLogFilePath() string {
	return os.Getenv("LOG_FILE_PATH")
}

// Get log file enabled flag
func EnvLogFileEnabled() bool {
	return os.Getenv("LOG_FILE_ENABLED") == "true"
}

// Get log file max size in MB
func EnvLogFileMaxSize() int {
	if v, err := strconv.Atoi(os.Getenv("LOG_FILE_MAX_SIZE_MB")); err == nil && v > 0 {
		return v
	}
	return 100 // default 100MB
}

// Get log file max backups
func EnvLogFileMaxBackups() int {
	if v, err := strconv.Atoi(os.Getenv("LOG_FILE_MAX_BACKUPS")); err == nil && v >= 0 {
		return v
	}
	return 3 // default 3 backups
}

// Get log file max age in days
func EnvLogFileMaxAge() int {
	if v, err := strconv.Atoi(os.Getenv("LOG_FILE_MAX_AGE_DAYS")); err == nil && v > 0 {
		return v
	}
	return 7 // default 7 days
}

/*
* XRPL protocol (compatible) server settings
 */
func EnvXrplWebsocketURL() string {
	return os.Getenv("XRPL_WEBSOCKET_URL")
}

func EnvXrplWebsocketFullHistoryURL() string {
	return os.Getenv("XRPL_WEBSOCKET_FULLHISTORY_URL")
}

/*
* ClickHouse settings
 */
func EnvClickHouseHost() string {
	v := os.Getenv("CLICKHOUSE_HOST")
	if v == "" {
		return "localhost"
	}
	return v
}

func EnvClickHousePort() int {
	if v, err := strconv.Atoi(os.Getenv("CLICKHOUSE_PORT")); err == nil && v > 0 {
		return v
	}
	return 9000 // default native port
}

func EnvClickHouseDatabase() string {
	v := os.Getenv("CLICKHOUSE_DATABASE")
	if v == "" {
		return "xrpl"
	}
	return v
}

func EnvClickHouseUser() string {
	v := os.Getenv("CLICKHOUSE_USER")
	if v == "" {
		return "default"
	}
	return v
}

func EnvClickHousePassword() string {
	return os.Getenv("CLICKHOUSE_PASSWORD")
}

// Batch size for ClickHouse inserts
func EnvClickHouseBatchSize() int {
	if v, err := strconv.Atoi(os.Getenv("CLICKHOUSE_BATCH_SIZE")); err == nil && v > 0 {
		return v
	}
	return 500 // default 500 rows per batch
}

// Batch timeout in milliseconds for ClickHouse inserts
func EnvClickHouseBatchTimeoutMs() int {
	if v, err := strconv.Atoi(os.Getenv("CLICKHOUSE_BATCH_TIMEOUT_MS")); err == nil && v > 0 {
		return v
	}
	return 60000 // default 60 seconds (to allow batch to accumulate to CLICKHOUSE_BATCH_SIZE)
}

/*
* Detailed logging settings
 */

var (
	detailedLoggingLedgers     map[uint32]bool
	detailedLoggingLedgersOnce sync.Once
)

// EnvDetailedLoggingLedgers returns a map of ledger indices that should be logged in detail
// Reads from DETAILED_LOGGING_LEDGERS env variable (comma-separated list)
// Example: DETAILED_LOGGING_LEDGERS=98900000,98900001,98900002
func EnvDetailedLoggingLedgers() map[uint32]bool {
	detailedLoggingLedgersOnce.Do(func() {
		detailedLoggingLedgers = make(map[uint32]bool)
		envValue := os.Getenv("DETAILED_LOGGING_LEDGERS")
		if envValue == "" {
			return
		}

		// Parse comma-separated list
		parts := strings.Split(envValue, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if ledgerIndex, err := strconv.ParseUint(part, 10, 32); err == nil {
				detailedLoggingLedgers[uint32(ledgerIndex)] = true
			}
		}
	})
	return detailedLoggingLedgers
}

// ShouldLogDetailed checks if a ledger index should be logged in detail
func ShouldLogDetailed(ledgerIndex uint32) bool {
	ledgers := EnvDetailedLoggingLedgers()
	return ledgers[ledgerIndex]
}

// Removed: Elasticsearch settings are no longer used

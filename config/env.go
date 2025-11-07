package config

import (
	"log"
	"os"
	"strconv"

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
			log.Fatalf("Error loading configuration file: %s", filename)
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
* Kafka settings
 */
func EnvKafkaBootstrapServer() string {
	return os.Getenv("KAFKA_BOOTSTRAP_SERVER")
}

func EnvKafkaGroupId() string {
	return os.Getenv("KAFKA_GROUP_ID")
}

func EnvKafkaTopicNamespace() string {
	return os.Getenv("KAFKA_TOPIC_NAMESPACE")
}

// Writer batching and delivery settings
func EnvKafkaWriterBatchSize() int {
	if v, err := strconv.Atoi(os.Getenv("KAFKA_WRITER_BATCH_SIZE")); err == nil && v > 0 {
		return v
	}
	return 100 // default
}

func EnvKafkaWriterBatchBytes() int {
	if v, err := strconv.Atoi(os.Getenv("KAFKA_WRITER_BATCH_BYTES")); err == nil && v > 0 {
		return v
	}
	return 1024 * 1024 // 1 MiB default
}

func EnvKafkaWriterBatchTimeoutMs() int {
	if v, err := strconv.Atoi(os.Getenv("KAFKA_WRITER_BATCH_TIMEOUT_MS")); err == nil && v >= 0 {
		return v
	}
	return 50 // default 50ms
}

func EnvKafkaWriterCompression() string {
	v := os.Getenv("KAFKA_WRITER_COMPRESSION")
	if v == "" {
		return "snappy" // lightweight default
	}
	return v
}

func EnvKafkaWriterRequiredAcks() int {
	if v, err := strconv.Atoi(os.Getenv("KAFKA_WRITER_REQUIRED_ACKS")); err == nil {
		// allow -1, 0, 1
		if v == -1 || v == 0 || v == 1 {
			return v
		}
	}
	return 1 // leader-only default
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
	return 5000 // default 1000 rows per batch
}

// Batch timeout in milliseconds for ClickHouse inserts
func EnvClickHouseBatchTimeoutMs() int {
	if v, err := strconv.Atoi(os.Getenv("CLICKHOUSE_BATCH_TIMEOUT_MS")); err == nil && v > 0 {
		return v
	}
	return 5000 // default 1 second
}

// Removed: Elasticsearch settings are no longer used

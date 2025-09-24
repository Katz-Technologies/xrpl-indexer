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
* Elasticsearch settings
 */

func EnvEsNamespace() string {
	return os.Getenv("ELASTICSEARCH_NAMESPACE")
}

func EnvEsURL() string {
	return os.Getenv("ELASTICSEARCH_URL")
}

func EnvEsUsername() string {
	return os.Getenv("ELASTICSEARCH_USERNAME")
}

func EnvEsPassword() string {
	return os.Getenv("ELASTICSEARCH_PASSWORD")
}

func EnvEsFingerprint() string {
	return os.Getenv("ELASTICSEARCH_FINGERPRINT")
}

func EnvEsFlushInterval() int {
	esFlushInterval, err := strconv.Atoi(os.Getenv("ELASTICSEARCH_FLUSH_INTERVAL"))
	if err != nil {
		return 5
	}
	return esFlushInterval
}

package connections

// Kafka functionality has been removed - data is written directly to ClickHouse
// This file is kept for compatibility but all functions are no-ops

var KafkaWriter interface{} = nil

func NewWriter() {
	// Kafka writer is no longer used - data is written directly to ClickHouse
}

var KafkaReaderLedger interface{} = nil
var KafkaReaderTransaction interface{} = nil
var KafkaReaderValidation interface{} = nil
var KafkaReaderPeerStatus interface{} = nil
var KafkaReaderConsensus interface{} = nil
var KafkaReaderPathFind interface{} = nil
var KafkaReaderManifest interface{} = nil
var KafkaReaderServer interface{} = nil
var KafkaReaderDefault interface{} = nil

func NewLedgerReader() {
	// Kafka readers are no longer used
}

func NewTransactionReader() {
	// Kafka readers are no longer used
}

func NewValidationReader() {
	// Kafka readers are no longer used
}

func NewPeerStatusReader() {
	// Kafka readers are no longer used
}

func NewConsensusReader() {
	// Kafka readers are no longer used
}

func NewPathFindReader() {
	// Kafka readers are no longer used
}

func NewManifestReader() {
	// Kafka readers are no longer used
}

func NewServerReader() {
	// Kafka readers are no longer used
}

func NewDefaultReader() {
	// Kafka readers are no longer used
}

func NewReaders() {
	// Kafka readers are no longer used
}

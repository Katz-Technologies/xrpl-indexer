-- Ensure database exists
CREATE DATABASE IF NOT EXISTS xrpl;

-- Kafka engines (raw topics)
CREATE TABLE IF NOT EXISTS xrpl.tx_kafka
(
  `key` String,
  `value` String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-broker1:29092',
         kafka_topic_list = 'xrpl-platform-transactions-processed',
         kafka_group_name = 'clickhouse-tx',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 4;

CREATE TABLE IF NOT EXISTS xrpl.ledger_kafka
(
  `key` String,
  `value` String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-broker1:29092',
         kafka_topic_list = 'xrpl-platform-ledgers',
         kafka_group_name = 'clickhouse-ledger',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 2;

CREATE TABLE IF NOT EXISTS xrpl.validation_kafka
(
  `key` String,
  `value` String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-broker1:29092',
         kafka_topic_list = 'xrpl-platform-validations',
         kafka_group_name = 'clickhouse-validation',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 2;

-- Target tables (MergeTree)
CREATE TABLE IF NOT EXISTS xrpl.tx
(
  hash String,
  ledger_index UInt32,
  date DateTime64(3, 'UTC'),
  validated UInt8,
  tx_type LowCardinality(String),
  account LowCardinality(String),
  destination LowCardinality(String),
  amount String,
  raw_json String CODEC(ZSTD)
)
ENGINE = MergeTree
PARTITION BY intDiv(ledger_index, 1000000)
ORDER BY (ledger_index, hash)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS xrpl.ledger
(
  ledger_index UInt32,
  close_time DateTime64(3, 'UTC'),
  txn_count UInt32,
  raw_json String CODEC(ZSTD)
)
ENGINE = MergeTree
PARTITION BY intDiv(ledger_index, 1000000)
ORDER BY (ledger_index)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS xrpl.validation
(
  validation_public_key LowCardinality(String),
  ledger_index UInt32,
  cookie String,
  raw_json String CODEC(ZSTD)
)
ENGINE = MergeTree
PARTITION BY intDiv(ledger_index, 1000000)
ORDER BY (ledger_index, validation_public_key, cookie)
SETTINGS index_granularity = 8192;

-- Materialized views
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.tx_mv
TO xrpl.tx AS
SELECT
  JSONExtractString(value, 'hash') AS hash,
  toUInt32(JSONExtract(value, 'ledger_index', 'Int64')) AS ledger_index,
  toDateTime64(JSONExtract(value, 'date', 'Float64'), 3, 'UTC') AS date,
  toUInt8(JSONExtract(value, 'validated', 'UInt8')) AS validated,
  JSONExtractString(value, 'TransactionType') AS tx_type,
  JSONExtractString(value, 'Account') AS account,
  JSONExtractString(value, 'Destination') AS destination,
  value AS raw_json
FROM xrpl.tx_kafka;

CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.ledger_mv
TO xrpl.ledger AS
SELECT
  toUInt32(JSONExtract(value, 'ledger_index', 'Int64')) AS ledger_index,
  toDateTime64(JSONExtract(value, 'close_time', 'Float64'), 3, 'UTC') AS close_time,
  toUInt32(JSONExtract(value, 'txn_count', 'Int64')) AS txn_count,
  value AS raw_json
FROM xrpl.ledger_kafka;

CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.validation_mv
TO xrpl.validation AS
SELECT
  JSONExtractString(value, 'validation_public_key') AS validation_public_key,
  toUInt32(JSONExtract(value, 'ledger_index', 'Int64')) AS ledger_index,
  JSONExtractString(value, 'cookie') AS cookie,
  value AS raw_json
FROM xrpl.validation_kafka;



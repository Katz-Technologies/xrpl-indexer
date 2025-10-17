-- ======================================================
-- XRPL ClickHouse Schema v2 (deduplicated)
-- Optimized for ClickHouse 24.8+
-- - Automatic deduplication via MV and settings
-- - Monthly partitions, ZSTD codec
-- ======================================================

CREATE DATABASE IF NOT EXISTS xrpl;

-- ==============================
-- XRP Prices
-- ==============================
CREATE TABLE IF NOT EXISTS xrpl.xrp_prices
(
  timestamp DateTime64(3, 'UTC'),
  price_usd Decimal(18, 6) CODEC(ZSTD(3)),
  version UInt64 DEFAULT now64()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(timestamp)
ORDER BY timestamp
SETTINGS
  index_granularity = 8192,
  index_granularity_bytes = 10485760;

-- ============================
-- Transactions
-- ============================
CREATE TABLE IF NOT EXISTS xrpl.transactions
(
  tx_id UUID,
  hash FixedString(64),
  ledger_index UInt32,
  in_ledger_index UInt32,
  close_time DateTime64(3, 'UTC'),
  tx_type LowCardinality(String),
  account_id UUID,
  destination_id UUID,
  result LowCardinality(String),
  fee_drops UInt64,
  raw_json String CODEC(ZSTD(3)),
  version UInt64 DEFAULT now64()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(close_time)
ORDER BY (tx_id)
SETTINGS
  index_granularity = 8192,
  index_granularity_bytes = 10485760;

-- Secondary indexes
ALTER TABLE xrpl.transactions ADD INDEX idx_ledger_index (ledger_index) TYPE minmax GRANULARITY 4;
ALTER TABLE xrpl.transactions ADD INDEX idx_close_time (close_time) TYPE minmax GRANULARITY 4;
ALTER TABLE xrpl.transactions ADD INDEX idx_tx_type (tx_type) TYPE set(0) GRANULARITY 64;

-- ============================
-- Accounts
-- ============================
CREATE TABLE IF NOT EXISTS xrpl.accounts
(
  account_id UUID,
  address String,
  version UInt64 DEFAULT now64()
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (account_id)
SETTINGS
  index_granularity = 8192;

ALTER TABLE xrpl.accounts ADD INDEX idx_address (address) TYPE tokenbf_v1(512, 3, 0) GRANULARITY 64;

-- ============================
-- Assets
-- ============================
CREATE TABLE IF NOT EXISTS xrpl.assets
(
  asset_id UUID,
  asset_type Enum8('XRP' = 0, 'IOU' = 1),
  currency String,
  issuer_id UUID,
  symbol LowCardinality(String),
  version UInt64 DEFAULT now64()
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (asset_id)
SETTINGS
  index_granularity = 8192;

-- Seed XRP
INSERT INTO xrpl.assets (asset_id, asset_type, currency, issuer_id, symbol)
VALUES ('7ab3a23b-28ba-5fb4-aac1-b3546017b182', 'XRP', 'XRP', '00000000-0000-0000-0000-000000000000', 'XRP');

INSERT INTO xrpl.accounts (account_id, address)
VALUES ('00000000-0000-0000-0000-000000000000', 'XRP');

-- ============================
-- Money Flow
-- ============================
CREATE TABLE IF NOT EXISTS xrpl.money_flow
(
  tx_id UUID,
  from_id UUID,
  to_id UUID,
  from_asset_id UUID,
  to_asset_id UUID,
  from_amount Decimal(38, 18) CODEC(ZSTD(3)),
  to_amount Decimal(38, 18) CODEC(ZSTD(3)),
  init_from_amount Decimal(38, 18) CODEC(ZSTD(3)),
  init_to_amount Decimal(38, 18) CODEC(ZSTD(3)),
  quote Decimal(38, 18) CODEC(ZSTD(3)),
  kind Enum8('transfer' = 0, 'dexOffer' = 1, 'swap' = 2),
  version UInt64 DEFAULT now64()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(toDateTime64(version / 1000, 3, 'UTC'))
ORDER BY (tx_id, from_id, to_id, from_asset_id, to_asset_id, kind, from_amount)
SETTINGS
  index_granularity = 8192,
  index_granularity_bytes = 10485760;

ALTER TABLE xrpl.money_flow ADD INDEX idx_from_to (from_id, to_id) TYPE set(0) GRANULARITY 64;
ALTER TABLE xrpl.money_flow ADD INDEX idx_kind (kind) TYPE set(0) GRANULARITY 64;
ALTER TABLE xrpl.money_flow ADD INDEX idx_assets (from_asset_id, to_asset_id) TYPE set(0) GRANULARITY 64;

-- =========================================================
-- Kafka ingestion (same topics)
-- =========================================================

CREATE TABLE IF NOT EXISTS xrpl.ch_tx_kafka (value String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'broker:29092',
         kafka_topic_list = 'xrpl-platform-ch-transactions',
         kafka_group_name = 'clickhouse-ch-tx',
         kafka_format = 'RawBLOB',
         kafka_num_consumers = 2,
         kafka_handle_error_mode = 'stream',
         kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS xrpl.ch_assets_kafka (value String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'broker:29092',
         kafka_topic_list = 'xrpl-platform-ch-assets',
         kafka_group_name = 'clickhouse-ch-asset',
         kafka_format = 'RawBLOB',
         kafka_num_consumers = 2,
         kafka_handle_error_mode = 'stream',
         kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS xrpl.ch_moneyflows_kafka (value String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'broker:29092',
         kafka_topic_list = 'xrpl-platform-ch-moneyflows',
         kafka_group_name = 'clickhouse-ch-mf',
         kafka_format = 'RawBLOB',
         kafka_num_consumers = 2,
         kafka_handle_error_mode = 'stream',
         kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS xrpl.ch_accounts_kafka (value String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'broker:29092',
         kafka_topic_list = 'xrpl-platform-ch-accounts',
         kafka_group_name = 'clickhouse-ch-accounts',
         kafka_format = 'RawBLOB',
         kafka_num_consumers = 1,
         kafka_handle_error_mode = 'stream',
         kafka_skip_broken_messages = 1;

-- ============================
-- Materialized Views (deduplicated)
-- ============================

CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.ch_mv_transactions TO xrpl.transactions AS
SELECT
  anyLast(toUUID(JSONExtractString(value, 'tx_id'))) AS tx_id,
  anyLast(JSONExtractString(value, 'hash')) AS hash,
  anyLast(toUInt32(JSONExtract(value, 'ledger_index', 'Int64'))) AS ledger_index,
  anyLast(toUInt32(JSONExtract(value, 'in_ledger_index', 'Int64'))) AS in_ledger_index,
  anyLast(toDateTime64(JSONExtract(value, 'close_time_unix', 'Int64'), 3, 'UTC')) AS close_time,
  anyLast(JSONExtractString(value, 'tx_type')) AS tx_type,
  anyLast(toUUID(JSONExtractString(value, 'account_id'))) AS account_id,
  anyLast(toUUID(JSONExtractString(value, 'destination_id'))) AS destination_id,
  anyLast(coalesce(JSONExtractString(value, 'result'), '')) AS result,
  anyLast(toUInt64(JSONExtract(value, 'fee_drops', 'Int64'))) AS fee_drops,
  anyLast(JSONExtractString(value, 'raw_json')) AS raw_json,
  now64() AS version
FROM xrpl.ch_tx_kafka
GROUP BY JSONExtractString(value, 'tx_id');

CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.ch_mv_assets TO xrpl.assets AS
SELECT
  anyLast(toUUID(JSONExtractString(value, 'asset_id'))) AS asset_id,
  anyLast(CAST(JSONExtractString(value, 'asset_type'), 'Enum8(''XRP''=0,''IOU''=1)')) AS asset_type,
  anyLast(JSONExtractString(value, 'currency')) AS currency,
  anyLast(toUUID(JSONExtractString(value, 'issuer_id'))) AS issuer_id,
  anyLast(coalesce(nullIf(JSONExtractString(value, 'symbol'), ''), JSONExtractString(value, 'currency'))) AS symbol,
  now64() AS version
FROM xrpl.ch_assets_kafka
GROUP BY JSONExtractString(value, 'asset_id');

CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.ch_mv_money_flows TO xrpl.money_flow AS
SELECT
  anyLast(toUUID(JSONExtractString(value, 'tx_id'))) AS tx_id,
  anyLast(toUUID(JSONExtractString(value, 'from_id'))) AS from_id,
  anyLast(toUUID(JSONExtractString(value, 'to_id'))) AS to_id,
  anyLast(toUUID(JSONExtractString(value, 'from_asset_id'))) AS from_asset_id,
  anyLast(toUUID(JSONExtractString(value, 'to_asset_id'))) AS to_asset_id,
  anyLast(CAST(JSONExtractString(value, 'from_amount'), 'Decimal(38,18)')) AS from_amount,
  anyLast(CAST(JSONExtractString(value, 'to_amount'), 'Decimal(38,18)')) AS to_amount,
  anyLast(CAST(JSONExtractString(value, 'init_from_amount'), 'Decimal(38,18)')) AS init_from_amount,
  anyLast(CAST(JSONExtractString(value, 'init_to_amount'), 'Decimal(38,18)')) AS init_to_amount,
  anyLast(CAST(JSONExtractString(value, 'quote'), 'Decimal(38,18)')) AS quote,
  anyLast(CAST(JSONExtractString(value, 'kind'), 'Enum8(''transfer''=0,''dexOffer''=1,''swap''=2)')) AS kind,
  now64() AS version
FROM xrpl.ch_moneyflows_kafka
GROUP BY (JSONExtractString(value, 'tx_id'), JSONExtractString(value, 'from_id'), JSONExtractString(value, 'to_id'));

CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.ch_mv_accounts TO xrpl.accounts AS
SELECT
  anyLast(toUUID(JSONExtractString(value, 'account_id'))) AS account_id,
  anyLast(JSONExtractString(value, 'address')) AS address,
  now64() AS version
FROM xrpl.ch_accounts_kafka
GROUP BY JSONExtractString(value, 'account_id');

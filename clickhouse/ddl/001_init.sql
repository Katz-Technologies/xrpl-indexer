-- Ensure database exists
CREATE DATABASE IF NOT EXISTS xrpl;

-- =====================
-- Transactions (unique)
-- =====================
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
  raw_json String CODEC(ZSTD)
)
ENGINE = ReplacingMergeTree
ORDER BY (tx_id);

-- ==================
-- Accounts (unique)
-- ==================
CREATE TABLE IF NOT EXISTS xrpl.accounts
(
  account_id UUID,
  address String
)
ENGINE = ReplacingMergeTree
ORDER BY (account_id);

-- =================
-- Assets (unique + XRP)
-- =================
CREATE TABLE IF NOT EXISTS xrpl.assets
(
  asset_id UUID,
  asset_type Enum8('XRP' = 0, 'IOU' = 1),
  currency String,
  issuer_id UUID,
  symbol LowCardinality(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (asset_id);

-- Seed XRP asset (id is deterministic)
-- Seed XRP asset (id is generated externally now if needed)

-- ============================================================
-- Money flows (relations between accounts for a given tx/asset)
-- tx_id + acc_1_id + acc_2_id + asset_id + amount + quote + kind
-- ============================================================
CREATE TABLE IF NOT EXISTS xrpl.money_flow
(
  tx_id UUID,
  from_id UUID,
  to_id UUID,
  from_asset_id UUID,
  to_asset_id UUID,
  from_amount Decimal(38, 18),
  to_amount Decimal(38, 18),
  quote Decimal(38, 18),
  kind Enum8('transfer' = 0, 'dexOffer' = 1, 'swap' = 2)
)
ENGINE = ReplacingMergeTree
ORDER BY (tx_id, from_id, to_id, from_asset_id, to_asset_id, kind, from_amount);

-- ==============================
-- Kafka ingestion (final rows)
-- ==============================

-- Kafka sources
CREATE TABLE IF NOT EXISTS xrpl.ch_tx_kafka (value String) ENGINE = Kafka SETTINGS kafka_broker_list = 'broker:29092', kafka_topic_list = 'xrpl-platform-ch-transactions', kafka_group_name = 'clickhouse-ch-tx', kafka_format = 'RawBLOB', kafka_num_consumers = 2, kafka_handle_error_mode = 'stream', kafka_skip_broken_messages = 1;
CREATE TABLE IF NOT EXISTS xrpl.ch_assets_kafka (value String) ENGINE = Kafka SETTINGS kafka_broker_list = 'broker:29092', kafka_topic_list = 'xrpl-platform-ch-assets', kafka_group_name = 'clickhouse-ch-asset', kafka_format = 'RawBLOB', kafka_num_consumers = 2, kafka_handle_error_mode = 'stream', kafka_skip_broken_messages = 1;
CREATE TABLE IF NOT EXISTS xrpl.ch_moneyflows_kafka (value String) ENGINE = Kafka SETTINGS kafka_broker_list = 'broker:29092', kafka_topic_list = 'xrpl-platform-ch-moneyflows', kafka_group_name = 'clickhouse-ch-mf', kafka_format = 'RawBLOB', kafka_num_consumers = 2, kafka_handle_error_mode = 'stream', kafka_skip_broken_messages = 1;
CREATE TABLE IF NOT EXISTS xrpl.ch_accounts_kafka (value String) ENGINE = Kafka SETTINGS kafka_broker_list = 'broker:29092', kafka_topic_list = 'xrpl-platform-ch-accounts', kafka_group_name = 'clickhouse-ch-accounts', kafka_format = 'RawBLOB', kafka_num_consumers = 1, kafka_handle_error_mode = 'stream', kafka_skip_broken_messages = 1;

-- Transactions MV: direct mapping from final JSON
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.ch_mv_transactions TO xrpl.transactions AS
SELECT
  toUUID(JSONExtractString(value, 'tx_id')) AS tx_id,
  JSONExtractString(value, 'hash') AS hash,
  toUInt32(JSONExtract(value, 'ledger_index', 'Int64')) AS ledger_index,
  toUInt32(JSONExtract(value, 'in_ledger_index', 'Int64')) AS in_ledger_index,
  toDateTime64(JSONExtract(value, 'close_time_unix', 'Int64'), 3, 'UTC') AS close_time,
  JSONExtractString(value, 'tx_type') AS tx_type,
  toUUID(JSONExtractString(value, 'account_id')) AS account_id,
  toUUID(JSONExtractString(value, 'destination_id')) AS destination_id,
  coalesce(JSONExtractString(value, 'result'), '') AS result,
  toUInt64(JSONExtract(value, 'fee_drops', 'Int64')) AS fee_drops,
  JSONExtractString(value, 'raw_json') AS raw_json
FROM xrpl.ch_tx_kafka;

-- Assets MV: direct mapping from final JSON
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.ch_mv_assets TO xrpl.assets AS
SELECT
  toUUID(JSONExtractString(value, 'asset_id')) AS asset_id,
  CAST(JSONExtractString(value, 'asset_type'), 'Enum8(''XRP''=0,''IOU''=1)') AS asset_type,
  JSONExtractString(value, 'currency') AS currency,
  toUUID(JSONExtractString(value, 'issuer_id')) AS issuer_id,
  coalesce(nullIf(JSONExtractString(value, 'symbol'), ''), JSONExtractString(value, 'currency')) AS symbol
FROM xrpl.ch_assets_kafka;

-- Money flow MV: direct mapping from final JSON
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.ch_mv_money_flows TO xrpl.money_flow AS
SELECT
  toUUID(JSONExtractString(value, 'tx_id')) AS tx_id,
  toUUID(JSONExtractString(value, 'from_id')) AS from_id,
  toUUID(JSONExtractString(value, 'to_id')) AS to_id,
  toUUID(JSONExtractString(value, 'from_asset_id')) AS from_asset_id,
  toUUID(JSONExtractString(value, 'to_asset_id')) AS to_asset_id,
  CAST(JSONExtractString(value, 'from_amount'), 'Decimal(38,18)') AS from_amount,
  CAST(JSONExtractString(value, 'to_amount'), 'Decimal(38,18)') AS to_amount,
  CAST(JSONExtractString(value, 'quote'), 'Decimal(38,18)') AS quote,
  CAST(JSONExtractString(value, 'kind'), 'Enum8(''transfer''=0,''dexOffer''=1,''swap''=2)') AS kind
FROM xrpl.ch_moneyflows_kafka;

-- Accounts MV: direct mapping from final JSON
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.ch_mv_accounts TO xrpl.accounts AS
SELECT
  toUUID(JSONExtractString(value, 'account_id')) AS account_id,
  JSONExtractString(value, 'address') AS address
FROM xrpl.ch_accounts_kafka;



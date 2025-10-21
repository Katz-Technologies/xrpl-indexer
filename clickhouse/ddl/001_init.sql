-- ======================================================
-- XRPL ClickHouse Schema v3 (без UUID)
-- Использует естественные уникальные поля XRPL
-- ======================================================

-- ==============================
-- XRP Prices (без изменений)
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
-- Assets таблица удалена - больше не нужна
-- ============================

-- ============================
-- Money Flow (объединенная таблица с данными транзакций)
-- ============================
CREATE TABLE IF NOT EXISTS xrpl.money_flow
(
  tx_hash FixedString(64),                 -- PRIMARY KEY вместо tx_id UUID
  ledger_index UInt32,                     -- из transactions
  in_ledger_index UInt32,                  -- из transactions
  close_time DateTime64(3, 'UTC'),         -- из transactions
  fee_drops UInt64,                        -- из transactions
  from_address String,                     -- адрес отправителя денежного потока
  to_address String,                       -- адрес получателя денежного потока
  from_currency String,                   -- валюта отправителя
  from_issuer_address String,              -- эмитент валюты отправителя
  to_currency String,                     -- валюта получателя
  to_issuer_address String,               -- эмитент валюты получателя
  from_amount Decimal(38, 18) CODEC(ZSTD(3)),
  to_amount Decimal(38, 18) CODEC(ZSTD(3)),
  init_from_amount Decimal(38, 18) CODEC(ZSTD(3)),
  init_to_amount Decimal(38, 18) CODEC(ZSTD(3)),
  quote Decimal(38, 18) CODEC(ZSTD(3)),
  kind Enum8('transfer' = 0, 'dexOffer' = 1, 'swap' = 2),
  version UInt64 DEFAULT now64()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(close_time)
ORDER BY (tx_hash, ledger_index, from_address, to_address, from_currency, from_issuer_address, to_currency, to_issuer_address, kind, from_amount)
SETTINGS
  index_granularity = 8192,
  index_granularity_bytes = 10485760;

-- Secondary indexes
ALTER TABLE xrpl.money_flow ADD INDEX idx_ledger_index (ledger_index) TYPE minmax GRANULARITY 4;
ALTER TABLE xrpl.money_flow ADD INDEX idx_close_time (close_time) TYPE minmax GRANULARITY 4;
ALTER TABLE xrpl.money_flow ADD INDEX idx_from_to (from_address, to_address) TYPE set(0) GRANULARITY 64;
ALTER TABLE xrpl.money_flow ADD INDEX idx_kind (kind) TYPE set(0) GRANULARITY 64;
ALTER TABLE xrpl.money_flow ADD INDEX idx_assets (from_currency, from_issuer_address, to_currency, to_issuer_address) TYPE set(0) GRANULARITY 64;

-- =========================================================
-- Kafka ingestion (обновленные таблицы без UUID)
-- =========================================================

-- Убрано ch_assets_kafka - таблицы assets больше нет

CREATE TABLE IF NOT EXISTS xrpl.ch_moneyflows_kafka (value String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'broker:29092',
         kafka_topic_list = 'xrpl-platform-ch-moneyflows',
         kafka_group_name = 'clickhouse-ch-mf',
         kafka_format = 'RawBLOB',
         kafka_num_consumers = 2,
         kafka_handle_error_mode = 'stream',
         kafka_skip_broken_messages = 1;

-- ============================
-- Materialized Views (обновленные без UUID)
-- ============================

-- Убрано ch_mv_assets - таблицы assets больше нет

CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.ch_mv_money_flows TO xrpl.money_flow AS
SELECT
  anyLast(JSONExtractString(value, 'tx_hash')) AS tx_hash,
  anyLast(toUInt32(JSONExtract(value, 'ledger_index', 'Int64'))) AS ledger_index,
  anyLast(toUInt32(JSONExtract(value, 'in_ledger_index', 'Int64'))) AS in_ledger_index,
  anyLast(toDateTime64(JSONExtract(value, 'close_time_unix', 'Int64'), 3, 'UTC')) AS close_time,
  anyLast(toUInt64(JSONExtract(value, 'fee_drops', 'Int64'))) AS fee_drops,
  anyLast(JSONExtractString(value, 'from_address')) AS from_address,
  anyLast(JSONExtractString(value, 'to_address')) AS to_address,
  anyLast(JSONExtractString(value, 'from_currency')) AS from_currency,
  anyLast(JSONExtractString(value, 'from_issuer_address')) AS from_issuer_address,
  anyLast(JSONExtractString(value, 'to_currency')) AS to_currency,
  anyLast(JSONExtractString(value, 'to_issuer_address')) AS to_issuer_address,
  anyLast(CAST(JSONExtractString(value, 'from_amount'), 'Decimal(38,18)')) AS from_amount,
  anyLast(CAST(JSONExtractString(value, 'to_amount'), 'Decimal(38,18)')) AS to_amount,
  anyLast(CAST(JSONExtractString(value, 'init_from_amount'), 'Decimal(38,18)')) AS init_from_amount,
  anyLast(CAST(JSONExtractString(value, 'init_to_amount'), 'Decimal(38,18)')) AS init_to_amount,
  anyLast(CAST(JSONExtractString(value, 'quote'), 'Decimal(38,18)')) AS quote,
  anyLast(CAST(JSONExtractString(value, 'kind'), 'Enum8(''transfer''=0,''dexOffer''=1,''swap''=2)')) AS kind,
  now64() AS version
FROM xrpl.ch_moneyflows_kafka
GROUP BY (
  JSONExtractString(value, 'tx_hash'), 
  JSONExtractString(value, 'from_address'), 
  JSONExtractString(value, 'to_address'),
  JSONExtractString(value, 'from_currency'),
  JSONExtractString(value, 'from_issuer_address'),
  JSONExtractString(value, 'to_currency'),
  JSONExtractString(value, 'to_issuer_address')
);

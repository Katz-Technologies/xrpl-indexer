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
-- Empty Ledgers (леджеры без Payment транзакций)
-- ============================
CREATE TABLE IF NOT EXISTS xrpl.empty_ledgers
(
  ledger_index UInt32,
  close_time DateTime64(3, 'UTC'),
  total_transactions UInt32,
  checked_at DateTime64(3, 'UTC') DEFAULT now64(),
  version UInt64 DEFAULT now64()
)
ENGINE = ReplacingMergeTree(version)
ORDER BY ledger_index
SETTINGS
  index_granularity = 8192,
  index_granularity_bytes = 10485760;

-- Secondary index for empty ledgers
ALTER TABLE xrpl.empty_ledgers ADD INDEX idx_ledger_index (ledger_index) TYPE minmax GRANULARITY 4;
ALTER TABLE xrpl.empty_ledgers ADD INDEX idx_close_time (close_time) TYPE minmax GRANULARITY 4;

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
  kind Enum8('unknown' = 0, 'transfer' = 1, 'dexOffer' = 2, 'swap' = 3, 'fee' = 4, 'burn' = 5, 'loss' = 6, 'payout' = 7),
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

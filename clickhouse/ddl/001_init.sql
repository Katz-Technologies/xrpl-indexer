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

-- Secondary indexes for empty ledgers
-- Note: These indexes may already exist. If you get an error, the indexes are already created.
-- You can safely ignore "index with this name already exists" errors or run these manually once.
ALTER TABLE xrpl.empty_ledgers ADD INDEX IF NOT EXISTS idx_ledger_index (ledger_index) TYPE minmax GRANULARITY 4;
ALTER TABLE xrpl.empty_ledgers ADD INDEX IF NOT EXISTS idx_close_time (close_time) TYPE minmax GRANULARITY 4;

-- Try to add indexes (will fail silently if they exist - this is expected)
-- Using a workaround: check system.data_skipping_indices first
-- For now, comment out and create manually if needed, or use the script below

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

-- Secondary indexes for money_flow
-- Note: These indexes may already exist. If you get an error, the indexes are already created.
-- You can safely ignore "index with this name already exists" errors or run these manually once.
ALTER TABLE xrpl.money_flow ADD INDEX IF NOT EXISTS idx_ledger_index (ledger_index) TYPE minmax GRANULARITY 4;
ALTER TABLE xrpl.money_flow ADD INDEX IF NOT EXISTS idx_close_time (close_time) TYPE minmax GRANULARITY 4;
ALTER TABLE xrpl.money_flow ADD INDEX IF NOT EXISTS idx_from_to (from_address, to_address) TYPE set(0) GRANULARITY 64;
ALTER TABLE xrpl.money_flow ADD INDEX IF NOT EXISTS idx_kind (kind) TYPE set(0) GRANULARITY 64;
ALTER TABLE xrpl.money_flow ADD INDEX IF NOT EXISTS idx_assets (from_currency, from_issuer_address, to_currency, to_issuer_address) TYPE set(0) GRANULARITY 64;

-- ============================
-- Known Tokens (известные токены)
-- ============================
CREATE TABLE IF NOT EXISTS xrpl.known_tokens
(
  currency String,
  issuer String,
  version UInt64 DEFAULT now64()
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (currency, issuer)
SETTINGS
  index_granularity = 8192,
  index_granularity_bytes = 10485760;

-- Secondary index for known tokens
-- Note: This index may already exist. If you get an error, the index is already created.
-- You can safely ignore "index with this name already exists" errors or run this manually once.
ALTER TABLE xrpl.known_tokens ADD INDEX IF NOT EXISTS idx_currency_issuer (currency, issuer) TYPE set(0) GRANULARITY 64;

-- ============================
-- Subscription Links (таблица связка подписок)
-- ============================
CREATE TABLE IF NOT EXISTS xrpl.subscription_links
(
  from_address String,                  -- адрес, который подписан
  to_address String,               -- адрес на который подписан
)
ENGINE = MergeTree()
ORDER BY (from_address, to_address)
SETTINGS
  index_granularity = 8192,
  index_granularity_bytes = 10485760;

-- Secondary indexes for subscription_links
-- Note: These indexes may already exist. If you get an error, the indexes are already created.
-- You can safely ignore "index with this name already exists" errors or run these manually once.
ALTER TABLE xrpl.subscription_links ADD INDEX IF NOT EXISTS idx_subscriber (from_address) TYPE set(0) GRANULARITY 64;
ALTER TABLE xrpl.subscription_links ADD INDEX IF NOT EXISTS idx_subscribed_to (to_address) TYPE set(0) GRANULARITY 64;

CREATE TABLE IF NOT EXISTS xrpl.new_tokens
(
  currency_code String,
  issuer String,
  first_seen_ledger_index UInt32,
  first_seen_in_ledger_index UInt32,
  create_timestamp DateTime64(3, 'UTC'),
)
ENGINE = MergeTree()
ORDER BY (first_seen_ledger_index, first_seen_in_ledger_index)
SETTINGS
  index_granularity = 8192,
  index_granularity_bytes = 10485760;

ALTER TABLE xrpl.new_tokens ADD INDEX IF NOT EXISTS idx_first_seen_ledger_index (first_seen_ledger_index) TYPE minmax GRANULARITY 4;
ALTER TABLE xrpl.new_tokens ADD INDEX IF NOT EXISTS idx_first_seen_in_ledger_index (first_seen_in_ledger_index) TYPE minmax GRANULARITY 4;

-- ============================
-- Token OHLC Prices (цены токенов из Sologenic API)
-- ============================
CREATE TABLE IF NOT EXISTS xrpl.token_ohlc
(
  timestamp DateTime64(3, 'UTC'),
  currency String,
  issuer String,
  open Decimal(38, 18) CODEC(ZSTD(3)),
  high Decimal(38, 18) CODEC(ZSTD(3)),
  low Decimal(38, 18) CODEC(ZSTD(3)),
  close Decimal(38, 18) CODEC(ZSTD(3)),
  volume Decimal(38, 18) CODEC(ZSTD(3)),
  version UInt64 DEFAULT now64()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (currency, issuer, timestamp)
SETTINGS
  index_granularity = 8192,
  index_granularity_bytes = 10485760;

-- Secondary indexes for token_ohlc
ALTER TABLE xrpl.token_ohlc ADD INDEX IF NOT EXISTS idx_timestamp (timestamp) TYPE minmax GRANULARITY 4;
ALTER TABLE xrpl.token_ohlc ADD INDEX IF NOT EXISTS idx_currency_issuer (currency, issuer) TYPE set(0) GRANULARITY 64;
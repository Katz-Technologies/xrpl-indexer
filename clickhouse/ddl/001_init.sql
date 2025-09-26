-- Ensure database exists
CREATE DATABASE IF NOT EXISTS xrpl;

-- Kafka engine (raw transactions only)
CREATE TABLE IF NOT EXISTS xrpl.tx_kafka
(
  `value` String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-broker1:29092',
         kafka_topic_list = 'xrpl-platform-transactions-processed',
         kafka_group_name = 'clickhouse-tx',
         kafka_format = 'RawBLOB',
         kafka_num_consumers = 4,
         kafka_handle_error_mode = 'stream',
         kafka_skip_broken_messages = 1;

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

-- Remove ledger and validation tables

-- Materialized view for transactions only
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.tx_mv
TO xrpl.tx AS
SELECT
  JSONExtractString(value, 'hash') AS hash,
  toUInt32(JSONExtract(value, 'ledger_index', 'Int64')) AS ledger_index,
  toDateTime64(JSONExtract(value, 'date', 'Float64') + 946684800, 3, 'UTC') AS date,
  toUInt8(JSONExtractBool(value, 'validated')) AS validated,
  JSONExtractString(value, 'TransactionType') AS tx_type,
  JSONExtractString(value, 'Account') AS account,
  JSONExtractString(value, 'Destination') AS destination,
  value AS raw_json
FROM xrpl.tx_kafka;

-- Normalized analytics schema built from processed transactions
-- Assumes kafka topic name namespace xrpl-platform and tx_kafka from 001_init.sql

CREATE TABLE IF NOT EXISTS xrpl.accounts_agg
(
  account_id Int64,
  address String,
  labels String,
  first_seen_state AggregateFunction(min, DateTime64(3, 'UTC')),
  last_seen_state AggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
ORDER BY (account_id);

CREATE VIEW IF NOT EXISTS xrpl.accounts AS
SELECT
  account_id,
  any(address) AS address,
  any(labels) AS labels,
  minMerge(first_seen_state) AS first_seen,
  maxMerge(last_seen_state) AS last_seen
FROM xrpl.accounts_agg
GROUP BY account_id;

CREATE TABLE IF NOT EXISTS xrpl.assets
(
  asset_id Int64,
  asset_type Enum8('XRP' = 0, 'IOU' = 1),
  currency String,
  issuer_id Int64,
  symbol LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (asset_id);

-- Seed XRP asset (id is deterministic)
INSERT INTO xrpl.assets (asset_id, asset_type, currency, issuer_id, symbol)
SELECT reinterpretAsInt64(sipHash64('XRP::')) AS asset_id, CAST('XRP', 'Enum8(''XRP''=0,''IOU''=1)') AS asset_type, 'XRP' AS currency, toInt64(0) AS issuer_id, 'XRP' AS symbol
WHERE NOT EXISTS (SELECT 1 FROM xrpl.assets WHERE asset_id = reinterpretAsInt64(sipHash64('XRP::')));

-- Register IOU assets from SendMax (path payments - source asset)
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.mv_assets_iou_from_sendmax
TO xrpl.assets AS
SELECT
  reinterpretAsInt64(sipHash64(concat('IOU:', JSON_VALUE(value, '$.SendMax.currency'), ':', JSON_VALUE(value, '$.SendMax.issuer')))) AS asset_id,
  CAST('IOU', 'Enum8(''XRP''=0,''IOU''=1)') AS asset_type,
  JSON_VALUE(value, '$.SendMax.currency') AS currency,
  reinterpretAsInt64(sipHash64(JSON_VALUE(value, '$.SendMax.issuer'))) AS issuer_id,
  coalesce(nullIf(JSON_VALUE(value, '$.SendMax._currency'), ''), JSON_VALUE(value, '$.SendMax.currency')) AS symbol
FROM xrpl.tx_kafka
WHERE JSONExtractString(value, 'TransactionType') = 'Payment'
  AND NOT isNull(JSONExtractRaw(value, 'SendMax'))
  AND coalesce(JSON_VALUE(value, '$.SendMax.native'), 'false') IN ('false','0')
  AND length(coalesce(JSON_VALUE(value, '$.SendMax.issuer'), '')) > 0
GROUP BY asset_id, asset_type, currency, issuer_id, symbol;

-- Register IOU assets from OfferCreate (orderbook sides)
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.mv_assets_iou_from_offer_gets
TO xrpl.assets AS
SELECT
  reinterpretAsInt64(sipHash64(concat('IOU:', JSON_VALUE(value, '$.TakerGets.currency'), ':', JSON_VALUE(value, '$.TakerGets.issuer')))) AS asset_id,
  CAST('IOU', 'Enum8(''XRP''=0,''IOU''=1)') AS asset_type,
  JSON_VALUE(value, '$.TakerGets.currency') AS currency,
  reinterpretAsInt64(sipHash64(JSON_VALUE(value, '$.TakerGets.issuer'))) AS issuer_id,
  coalesce(nullIf(JSON_VALUE(value, '$.TakerGets._currency'), ''), JSON_VALUE(value, '$.TakerGets.currency')) AS symbol
FROM xrpl.tx_kafka
WHERE JSONExtractString(value, 'TransactionType') = 'OfferCreate'
  AND NOT isNull(JSONExtractRaw(value, 'TakerGets'))
  AND coalesce(JSON_VALUE(value, '$.TakerGets.native'), 'false') IN ('false','0')
  AND length(coalesce(JSON_VALUE(value, '$.TakerGets.issuer'), '')) > 0
GROUP BY asset_id, asset_type, currency, issuer_id, symbol;

CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.mv_assets_iou_from_offer_pays
TO xrpl.assets AS
SELECT
  reinterpretAsInt64(sipHash64(concat('IOU:', JSON_VALUE(value, '$.TakerPays.currency'), ':', JSON_VALUE(value, '$.TakerPays.issuer')))) AS asset_id,
  CAST('IOU', 'Enum8(''XRP''=0,''IOU''=1)') AS asset_type,
  JSON_VALUE(value, '$.TakerPays.currency') AS currency,
  reinterpretAsInt64(sipHash64(JSON_VALUE(value, '$.TakerPays.issuer'))) AS issuer_id,
  coalesce(nullIf(JSON_VALUE(value, '$.TakerPays._currency'), ''), JSON_VALUE(value, '$.TakerPays.currency')) AS symbol
FROM xrpl.tx_kafka
WHERE JSONExtractString(value, 'TransactionType') = 'OfferCreate'
  AND NOT isNull(JSONExtractRaw(value, 'TakerPays'))
  AND coalesce(JSON_VALUE(value, '$.TakerPays.native'), 'false') IN ('false','0')
  AND length(coalesce(JSON_VALUE(value, '$.TakerPays.issuer'), '')) > 0
GROUP BY asset_id, asset_type, currency, issuer_id, symbol;

-- Register IOU assets from TrustSet (limit amount)
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.mv_assets_iou_from_trustset
TO xrpl.assets AS
SELECT
  reinterpretAsInt64(sipHash64(concat('IOU:', JSON_VALUE(value, '$.LimitAmount.currency'), ':', JSON_VALUE(value, '$.LimitAmount.issuer')))) AS asset_id,
  CAST('IOU', 'Enum8(''XRP''=0,''IOU''=1)') AS asset_type,
  JSON_VALUE(value, '$.LimitAmount.currency') AS currency,
  reinterpretAsInt64(sipHash64(JSON_VALUE(value, '$.LimitAmount.issuer'))) AS issuer_id,
  coalesce(nullIf(JSON_VALUE(value, '$.LimitAmount._currency'), ''), JSON_VALUE(value, '$.LimitAmount.currency')) AS symbol
FROM xrpl.tx_kafka
WHERE JSONExtractString(value, 'TransactionType') = 'TrustSet'
  AND NOT isNull(JSONExtractRaw(value, 'LimitAmount'))
  AND length(coalesce(JSON_VALUE(value, '$.LimitAmount.issuer'), '')) > 0
GROUP BY asset_id, asset_type, currency, issuer_id, symbol;

-- Register IOU assets from meta.delivered_amount (path payments - destination asset)
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.mv_assets_iou_from_meta_delivered
TO xrpl.assets AS
WITH JSONExtractRaw(value, 'meta') AS meta_raw,
     JSONExtractRaw(meta_raw, 'delivered_amount') AS d_raw
SELECT
  reinterpretAsInt64(sipHash64(concat('IOU:', JSON_VALUE(d_raw, '$.currency'), ':', JSON_VALUE(d_raw, '$.issuer')))) AS asset_id,
  CAST('IOU', 'Enum8(''XRP''=0,''IOU''=1)') AS asset_type,
  JSON_VALUE(d_raw, '$.currency') AS currency,
  reinterpretAsInt64(sipHash64(JSON_VALUE(d_raw, '$.issuer'))) AS issuer_id,
  coalesce(nullIf(JSON_VALUE(d_raw, '$._currency'), ''), JSON_VALUE(d_raw, '$.currency')) AS symbol
FROM xrpl.tx_kafka
WHERE JSONExtractString(value, 'TransactionType') = 'Payment'
  AND NOT isNull(d_raw)
  AND coalesce(JSON_VALUE(d_raw, '$.native'), 'false') IN ('false','0')
  AND length(coalesce(JSON_VALUE(d_raw, '$.issuer'), '')) > 0
GROUP BY asset_id, asset_type, currency, issuer_id, symbol;

CREATE TABLE IF NOT EXISTS xrpl.pairs
(
  pair_id Int64,
  base_id Int64,
  quote_id Int64
)
ENGINE = MergeTree
ORDER BY (pair_id);

CREATE TABLE IF NOT EXISTS xrpl.tx_min
(
  tx_id Int64,
  hash FixedString(64),
  ledger_index UInt32,
  close_time DateTime64(3, 'UTC'),
  type Enum8('Payment' = 0, 'OfferCreate' = 1, 'OfferCancel' = 2, 'TrustSet' = 3, 'AMM' = 4),
  account_id Int64,
  result LowCardinality(String),
  fee_drops UInt64,
  path_flags UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(close_time)
ORDER BY (ledger_index, tx_id);

-- tx_min from processed tx (xrpl.tx_kafka)
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.mv_tx_min
TO xrpl.tx_min AS
SELECT
  reinterpretAsInt64(sipHash64(JSONExtractString(value, 'hash'))) AS tx_id,
  JSONExtractString(value, 'hash') AS hash,
  toUInt32(JSONExtract(value, 'ledger_index', 'Int64')) AS ledger_index,
  toDateTime64(JSONExtract(value, 'date', 'Float64') + 946684800, 3, 'UTC') AS close_time,
  CAST(JSONExtractString(value, 'TransactionType'), 'Enum8(''Payment''=0,''OfferCreate''=1,''OfferCancel''=2,''TrustSet''=3,''AMM''=4)') AS type,
  reinterpretAsInt64(sipHash64(JSONExtractString(value, 'Account'))) AS account_id,
  coalesce(JSON_VALUE(value, '$.meta.TransactionResult'), '') AS result,
  toUInt64(JSONExtract(value, 'Fee', 'Int64')) AS fee_drops,
  toUInt32(JSONExtract(value, 'Flags', 'Int64')) AS path_flags
FROM xrpl.tx_kafka;

-- Accounts: seen sender and destination per tx
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.mv_accounts_seen_sender
TO xrpl.accounts_agg AS
SELECT
  reinterpretAsInt64(sipHash64(JSONExtractString(value, 'Account'))) AS account_id,
  any(JSONExtractString(value, 'Account')) AS address,
  any('{}') AS labels,
  minState(toDateTime64(JSONExtract(value, 'date', 'Float64') + 946684800, 3, 'UTC')) AS first_seen_state,
  maxState(toDateTime64(JSONExtract(value, 'date', 'Float64') + 946684800, 3, 'UTC')) AS last_seen_state
FROM xrpl.tx_kafka
GROUP BY account_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.mv_accounts_seen_destination
TO xrpl.accounts_agg AS
SELECT
  reinterpretAsInt64(sipHash64(JSONExtractString(value, 'Destination'))) AS account_id,
  any(JSONExtractString(value, 'Destination')) AS address,
  any('{}') AS labels,
  minState(toDateTime64(JSONExtract(value, 'date', 'Float64') + 946684800, 3, 'UTC')) AS first_seen_state,
  maxState(toDateTime64(JSONExtract(value, 'date', 'Float64') + 946684800, 3, 'UTC')) AS last_seen_state
FROM xrpl.tx_kafka
WHERE JSONExtractString(value, 'Destination') != ''
GROUP BY account_id;

CREATE TABLE IF NOT EXISTS xrpl.money_flows
(
  tx_id Int64,
  close_time DateTime64(3, 'UTC'),
  account_id Int64,
  asset_id Int64,
  pair_id Int64,
  kind Enum8('transfer' = 0, 'trade_orderbook' = 1, 'trade_amm' = 2, 'fee' = 3, 'issuer_op' = 4),
  role Enum8('taker' = 0, 'maker' = 1, 'amm' = 2, 'sender' = 3, 'receiver' = 4),
  delta Decimal(38, 18),
  seq_hint UInt32
)
ENGINE = ReplacingMergeTree
ORDER BY (tx_id, account_id, asset_id, pair_id, kind, role);

CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.mv_money_flows_fee
TO xrpl.money_flows AS
SELECT
  reinterpretAsInt64(sipHash64(JSONExtractString(value, 'hash'))) AS tx_id,
  toDateTime64(JSONExtract(value, 'date', 'Float64') + 946684800, 3, 'UTC') AS close_time,
  reinterpretAsInt64(sipHash64(JSONExtractString(value, 'Account'))) AS account_id,
  reinterpretAsInt64(sipHash64('XRP::')) AS asset_id,
  toInt64(0) AS pair_id,
  CAST('fee', 'Enum8(''transfer''=0,''trade_orderbook''=1,''trade_amm''=2,''fee''=3,''issuer_op''=4)') AS kind,
  CAST('sender', 'Enum8(''taker''=0,''maker''=1,''amm''=2,''sender''=3,''receiver''=4)') AS role,
  CAST(-toDecimal128(JSONExtract(value, 'Fee', 'Int64'), 6) / toDecimal128(1000000, 6), 'Decimal(38,18)') AS delta,
  toUInt32(JSONExtract(value, 'Sequence', 'Int64')) AS seq_hint
FROM xrpl.tx_kafka
WHERE JSONExtract(value, 'Fee', 'Int64') > 0;

-- Money flows (initial): XRP Payment send/receive legs (uses Amount field if native)
-- Sender leg: XRP native
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.mv_money_flows_payment_xrp_sender
TO xrpl.money_flows AS
WITH JSONExtractRaw(value, 'Amount') AS amount_raw
SELECT
  reinterpretAsInt64(sipHash64(JSONExtractString(value, 'hash'))) AS tx_id,
  toDateTime64(JSONExtract(value, 'date', 'Float64') + 946684800, 3, 'UTC') AS close_time,
  reinterpretAsInt64(sipHash64(JSONExtractString(value, 'Account'))) AS account_id,
  reinterpretAsInt64(sipHash64('XRP::')) AS asset_id,
  toInt64(0) AS pair_id,
  CAST('transfer', 'Enum8(''transfer''=0,''trade_orderbook''=1,''trade_amm''=2,''fee''=3,''issuer_op''=4)') AS kind,
  CAST('sender', 'Enum8(''taker''=0,''maker''=1,''amm''=2,''sender''=3,''receiver''=4)') AS role,
  CAST(-toDecimal128(toFloat64OrZero(JSON_VALUE(amount_raw, '$._value')), 18), 'Decimal(38,18)') AS delta,
  toUInt32(JSONExtract(value, 'Sequence', 'Int64')) AS seq_hint
FROM xrpl.tx_kafka
WHERE JSONExtractString(value, 'TransactionType') = 'Payment'
  AND coalesce(JSON_VALUE(amount_raw, '$.native'), 'false') IN ('true','1');

-- Receiver leg: XRP native
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.mv_money_flows_payment_xrp_receiver
TO xrpl.money_flows AS
WITH JSONExtractRaw(value, 'Amount') AS amount_raw
SELECT
  reinterpretAsInt64(sipHash64(JSONExtractString(value, 'hash'))) AS tx_id,
  toDateTime64(JSONExtract(value, 'date', 'Float64') + 946684800, 3, 'UTC') AS close_time,
  reinterpretAsInt64(sipHash64(JSONExtractString(value, 'Destination'))) AS account_id,
  reinterpretAsInt64(sipHash64('XRP::')) AS asset_id,
  toInt64(0) AS pair_id,
  CAST('transfer', 'Enum8(''transfer''=0,''trade_orderbook''=1,''trade_amm''=2,''fee''=3,''issuer_op''=4)') AS kind,
  CAST('receiver', 'Enum8(''taker''=0,''maker''=1,''amm''=2,''sender''=3,''receiver''=4)') AS role,
  CAST(toDecimal128(toFloat64OrZero(JSON_VALUE(amount_raw, '$._value')), 18), 'Decimal(38,18)') AS delta,
  toUInt32(JSONExtract(value, 'Sequence', 'Int64')) AS seq_hint
FROM xrpl.tx_kafka
WHERE JSONExtractString(value, 'TransactionType') = 'Payment'
  AND JSONExtractString(value, 'Destination') != ''
  AND coalesce(JSON_VALUE(amount_raw, '$.native'), 'false') IN ('true','1');

-- Daily balances materialization
-- Ensure target table exists before creating MV
CREATE TABLE IF NOT EXISTS xrpl.balances_daily
(
  day Date,
  account_id Int64,
  asset_id Int64,
  delta_day Decimal(38, 18)
)
ENGINE = SummingMergeTree
ORDER BY (day, account_id, asset_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.mv_balances_daily
TO xrpl.balances_daily AS
SELECT
  toDate(close_time) AS day,
  account_id,
  asset_id,
  sum(delta) AS delta_day
FROM xrpl.money_flows
GROUP BY day, account_id, asset_id;

-- ==============================
-- IOU Payments (direct transfers)
-- ==============================

-- Register IOU assets observed in Payment.Amount
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.mv_assets_iou_from_payments
TO xrpl.assets AS
SELECT
  reinterpretAsInt64(sipHash64(concat('IOU:', JSON_VALUE(value, '$.Amount.currency'), ':', JSON_VALUE(value, '$.Amount.issuer')))) AS asset_id,
  CAST('IOU', 'Enum8(''XRP''=0,''IOU''=1)') AS asset_type,
  JSON_VALUE(value, '$.Amount.currency') AS currency,
  reinterpretAsInt64(sipHash64(JSON_VALUE(value, '$.Amount.issuer'))) AS issuer_id,
  multiIf(length(coalesce(JSON_VALUE(value, '$.Amount._currency'), '')) > 0,
          JSON_VALUE(value, '$.Amount._currency'),
          JSON_VALUE(value, '$.Amount.currency')) AS symbol
FROM xrpl.tx_kafka
WHERE JSONExtractString(value, 'TransactionType') = 'Payment'
  AND coalesce(JSON_VALUE(value, '$.Amount.native'), 'false') IN ('false','0')
  AND length(coalesce(JSON_VALUE(value, '$.Amount.issuer'), '')) > 0
GROUP BY asset_id, asset_type, currency, issuer_id, symbol;

-- Money flows for IOU Payment (sender leg)
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.mv_money_flows_payment_iou_sender
TO xrpl.money_flows AS
WITH JSONExtractRaw(value, 'Amount') AS amount_raw
SELECT
  reinterpretAsInt64(sipHash64(JSONExtractString(value, 'hash'))) AS tx_id,
  toDateTime64(JSONExtract(value, 'date', 'Float64') + 946684800, 3, 'UTC') AS close_time,
  reinterpretAsInt64(sipHash64(JSONExtractString(value, 'Account'))) AS account_id,
  reinterpretAsInt64(sipHash64(concat('IOU:', JSON_VALUE(amount_raw, '$.currency'), ':', JSON_VALUE(amount_raw, '$.issuer')))) AS asset_id,
  toInt64(0) AS pair_id,
  CAST('transfer', 'Enum8(''transfer''=0,''trade_orderbook''=1,''trade_amm''=2,''fee''=3,''issuer_op''=4)') AS kind,
  CAST('sender', 'Enum8(''taker''=0,''maker''=1,''amm''=2,''sender''=3,''receiver''=4)') AS role,
  CAST(-toDecimal128(toFloat64OrZero(JSON_VALUE(amount_raw, '$._value')), 18), 'Decimal(38,18)') AS delta,
  toUInt32(JSONExtract(value, 'Sequence', 'Int64')) AS seq_hint
FROM xrpl.tx_kafka
WHERE JSONExtractString(value, 'TransactionType') = 'Payment'
  AND length(coalesce(JSON_VALUE(amount_raw, '$.issuer'), '')) > 0
  AND (
    isNull(JSONExtractRaw(value, 'SendMax'))
    OR (
      JSON_VALUE(value, '$.SendMax.currency') = JSON_VALUE(amount_raw, '$.currency')
      AND JSON_VALUE(value, '$.SendMax.issuer') = JSON_VALUE(amount_raw, '$.issuer')
    )
  );

-- Money flows for IOU Payment (receiver leg)
CREATE MATERIALIZED VIEW IF NOT EXISTS xrpl.mv_money_flows_payment_iou_receiver
TO xrpl.money_flows AS
WITH JSONExtractRaw(value, 'Amount') AS amount_raw
SELECT
  reinterpretAsInt64(sipHash64(JSONExtractString(value, 'hash'))) AS tx_id,
  toDateTime64(JSONExtract(value, 'date', 'Float64') + 946684800, 3, 'UTC') AS close_time,
  reinterpretAsInt64(sipHash64(JSONExtractString(value, 'Destination'))) AS account_id,
  reinterpretAsInt64(sipHash64(concat('IOU:', JSON_VALUE(amount_raw, '$.currency'), ':', JSON_VALUE(amount_raw, '$.issuer')))) AS asset_id,
  toInt64(0) AS pair_id,
  CAST('transfer', 'Enum8(''transfer''=0,''trade_orderbook''=1,''trade_amm''=2,''fee''=3,''issuer_op''=4)') AS kind,
  CAST('receiver', 'Enum8(''taker''=0,''maker''=1,''amm''=2,''sender''=3,''receiver''=4)') AS role,
  CAST(toDecimal128(toFloat64OrZero(JSON_VALUE(amount_raw, '$._value')), 18), 'Decimal(38,18)') AS delta,
  toUInt32(JSONExtract(value, 'Sequence', 'Int64')) AS seq_hint
FROM xrpl.tx_kafka
WHERE JSONExtractString(value, 'TransactionType') = 'Payment'
  AND length(coalesce(JSON_VALUE(amount_raw, '$.issuer'), '')) > 0
  AND length(JSONExtractString(value, 'Destination')) > 0
  AND (
    isNull(JSONExtractRaw(value, 'SendMax'))
    OR (
      JSON_VALUE(value, '$.SendMax.currency') = JSON_VALUE(amount_raw, '$.currency')
      AND JSON_VALUE(value, '$.SendMax.issuer') = JSON_VALUE(amount_raw, '$.issuer')
    )
  );



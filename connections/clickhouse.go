package connections

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/socketio"
)

var ClickHouseConn driver.Conn
var chOnce sync.Once
var chBatchWriter *ClickHouseBatchWriter

// MoneyFlowRow represents a money flow row for ClickHouse (duplicated from models to avoid circular import)
type MoneyFlowRow struct {
	TxHash            string
	LedgerIndex       uint32
	InLedgerIndex     uint32
	CloseTimeUnix     int64
	FeeDrops          uint64
	FromAddress       string
	ToAddress         string
	FromCurrency      string
	FromIssuerAddress string
	ToCurrency        string
	ToIssuerAddress   string
	FromAmount        string
	ToAmount          string
	InitFromAmount    string
	InitToAmount      string
	Quote             string
	Kind              string
	Version           uint64
}

// BatchFlushCallback is called after a successful batch flush
// It receives the batch of rows that were flushed
type BatchFlushCallback func(batch []MoneyFlowRow)

// ClickHouseBatchWriter handles batched writes to ClickHouse
type ClickHouseBatchWriter struct {
	conn           driver.Conn
	batchSize      int
	batchTimeout   time.Duration
	moneyFlowBatch []MoneyFlowRow
	mu             sync.Mutex
	flushTicker    *time.Ticker
	stopChan       chan struct{}
	wg             sync.WaitGroup
	stopped        bool
	stopOnce       sync.Once
	flushCallback  BatchFlushCallback // Optional callback after successful flush
}

// NewClickHouseConnection initializes ClickHouse connection
func NewClickHouseConnection() {
	chOnce.Do(func() {
		host := config.EnvClickHouseHost()
		port := config.EnvClickHousePort()
		database := config.EnvClickHouseDatabase()
		user := config.EnvClickHouseUser()
		password := config.EnvClickHousePassword()

		logger.Log.Info().
			Str("host", host).
			Int("port", port).
			Str("database", database).
			Str("user", user).
			Msg("Initializing ClickHouse connection")

		conn, err := clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", host, port)},
			Auth: clickhouse.Auth{
				Database: database,
				Username: user,
				Password: password,
			},
			Settings: clickhouse.Settings{
				"max_execution_time": 60,
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
		})

		if err != nil {
			logger.Log.Fatal().Err(err).Msg("Failed to connect to ClickHouse")
		}

		// Test connection
		if err := conn.Ping(context.Background()); err != nil {
			logger.Log.Fatal().Err(err).Msg("Failed to ping ClickHouse")
		}

		ClickHouseConn = conn
		logger.Log.Info().Msg("ClickHouse connection initialized successfully")

		// Initialize batch writer
		batchSize := config.EnvClickHouseBatchSize()
		batchTimeoutMs := config.EnvClickHouseBatchTimeoutMs()
		batchTimeout := time.Duration(batchTimeoutMs) * time.Millisecond
		logger.Log.Info().
			Int("batch_size", batchSize).
			Int("batch_timeout_ms", batchTimeoutMs).
			Dur("batch_timeout", batchTimeout).
			Msg("Initializing ClickHouse batch writer")
		chBatchWriter = NewClickHouseBatchWriter(conn, batchSize, batchTimeout)
		chBatchWriter.Start()
		logger.Log.Info().
			Int("batch_size", batchSize).
			Int("batch_timeout_ms", batchTimeoutMs).
			Msg("ClickHouse batch writer started - will accumulate money flow rows until batch size is reached")
	})
}

// NewClickHouseBatchWriter creates a new batch writer
func NewClickHouseBatchWriter(conn driver.Conn, batchSize int, batchTimeout time.Duration) *ClickHouseBatchWriter {
	return &ClickHouseBatchWriter{
		conn:           conn,
		batchSize:      batchSize,
		batchTimeout:   batchTimeout,
		moneyFlowBatch: make([]MoneyFlowRow, 0, batchSize),
		stopChan:       make(chan struct{}),
	}
}

// SetFlushCallback sets a callback function that will be called after each successful batch flush
func (w *ClickHouseBatchWriter) SetFlushCallback(callback BatchFlushCallback) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushCallback = callback
}

// SetBatchFlushCallback sets a callback function for the global batch writer
// This callback will be called after each successful batch flush
func SetBatchFlushCallback(callback BatchFlushCallback) {
	if chBatchWriter != nil {
		chBatchWriter.SetFlushCallback(callback)
	}
}

// Start starts the batch writer with periodic flushing
func (w *ClickHouseBatchWriter) Start() {
	// Start periodic flush ticker
	w.flushTicker = time.NewTicker(w.batchTimeout)
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-w.flushTicker.C:
				// Check batch size before flushing by timeout
				// Only flush if batch has accumulated a reasonable amount (at least 10% of target size)
				// This prevents flushing tiny batches while waiting for more data
				w.mu.Lock()
				batchSize := len(w.moneyFlowBatch)
				minFlushSize := w.batchSize / 10 // Flush only if at least 10% of target size
				if minFlushSize < 10 {
					minFlushSize = 10 // Minimum 10 rows
				}
				w.mu.Unlock()
				if batchSize >= minFlushSize {
					logger.Log.Debug().
						Int("batch_size", batchSize).
						Int("target_batch_size", w.batchSize).
						Int("min_flush_size", minFlushSize).
						Dur("timeout", w.batchTimeout).
						Msg("Flushing batch due to timeout")

					// Use recover to prevent panic from crashing the goroutine
					func() {
						defer func() {
							if r := recover(); r != nil {
								logger.Log.Error().
									Interface("panic", r).
									Int("batch_size", batchSize).
									Msg("Panic occurred during timeout flush - batch will be retried")
							}
						}()

						if err := w.Flush(); err != nil {
							logger.Log.Error().
								Err(err).
								Int("batch_size", batchSize).
								Msg("Failed to flush batch on timeout - batch will be retried")
							// Don't crash - batch is still in memory and will be retried
						}
					}()
				} else if batchSize > 0 {
					logger.Log.Trace().
						Int("batch_size", batchSize).
						Int("target_batch_size", w.batchSize).
						Int("min_flush_size", minFlushSize).
						Msg("Skipping timeout flush - batch too small, waiting for more data")
				}
			case <-w.stopChan:
				return
			}
		}
	}()
}

// Stop stops the batch writer and flushes remaining data
func (w *ClickHouseBatchWriter) Stop() {
	w.stopOnce.Do(func() {
		w.mu.Lock()
		w.stopped = true
		w.mu.Unlock()

		// Close channel only if not already closed
		select {
		case <-w.stopChan:
			// Channel already closed
		default:
			close(w.stopChan)
		}

		if w.flushTicker != nil {
			w.flushTicker.Stop()
		}
		w.wg.Wait()
		w.Flush() // Final flush
	})
}

// WriteMoneyFlow adds a money flow row to the batch
func (w *ClickHouseBatchWriter) WriteMoneyFlow(row MoneyFlowRow) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.moneyFlowBatch = append(w.moneyFlowBatch, row)
	currentBatchSize := len(w.moneyFlowBatch)

	// Flush if batch is full
	if currentBatchSize >= w.batchSize {
		logger.Log.Info().
			Int("batch_size", currentBatchSize).
			Int("target_batch_size", w.batchSize).
			Msg("Batch reached target size, flushing to ClickHouse")

		// Use recover to prevent panic from propagating and crashing the application
		var flushErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					flushErr = fmt.Errorf("panic during flush: %v", r)
					logger.Log.Error().
						Interface("panic", r).
						Int("batch_size", currentBatchSize).
						Msg("Panic occurred during batch flush in WriteMoneyFlow - batch will be retried")
				}
			}()
			flushErr = w.flushUnlocked()
		}()

		// If flush failed, log error but don't crash - batch will be retried later
		if flushErr != nil {
			logger.Log.Error().
				Err(flushErr).
				Int("batch_size", currentBatchSize).
				Msg("Failed to flush batch in WriteMoneyFlow - batch will be retried on next flush")
			// Don't return error here - the batch is still in memory and will be retried
			// This prevents the application from crashing and allows it to continue processing
		}

		return flushErr
	}

	return nil
}

// Flush flushes the current batch to ClickHouse
func (w *ClickHouseBatchWriter) Flush() error {
	w.mu.Lock()
	err := w.flushUnlocked()
	// flushUnlocked() guarantees the mutex is re-locked before returning
	// (including in case of panic), so we can safely unlock it here
	w.mu.Unlock()
	return err
}

// flushUnlocked flushes without locking (must be called with lock held)
func (w *ClickHouseBatchWriter) flushUnlocked() error {
	if len(w.moneyFlowBatch) == 0 {
		return nil
	}

	batch := w.moneyFlowBatch
	w.moneyFlowBatch = make([]MoneyFlowRow, 0, w.batchSize)

	// Release lock before database operation
	w.mu.Unlock()
	lockReleased := true

	// Use defer to ensure mutex is re-locked in all cases (including panics)
	// This prevents "unlock of unlocked mutex" errors
	defer func() {
		if lockReleased {
			w.mu.Lock()
		}
	}()

	// Recover from panics that might occur during batch operations
	// Convert panic to error to prevent worker from hanging
	var panicErr error
	defer func() {
		if r := recover(); r != nil {
			panicErr = fmt.Errorf("panic during ClickHouse batch flush: %v", r)
			logger.Log.Error().
				Interface("panic", r).
				Int("batch_size", len(batch)).
				Msg("Panic occurred during ClickHouse batch flush - converting to error")
			// Mutex will be re-locked by the defer above if it was released
		}
	}()

	// Calculate timeout based on batch size
	// Base timeout: 30 seconds, add 1 second per 50 rows
	// For large batches (600+ rows), use at least 60 seconds
	baseTimeout := 30 * time.Second
	batchSizeTimeout := time.Duration(len(batch)/50) * time.Second
	if batchSizeTimeout < 0 {
		batchSizeTimeout = 0
	}
	totalTimeout := baseTimeout + batchSizeTimeout
	if totalTimeout < 60*time.Second && len(batch) > 500 {
		totalTimeout = 60 * time.Second
	}
	if totalTimeout > 120*time.Second {
		totalTimeout = 120 * time.Second // Cap at 2 minutes
	}

	ctx, cancel := context.WithTimeout(context.Background(), totalTimeout)
	defer cancel()

	logger.Log.Debug().
		Int("batch_size", len(batch)).
		Dur("timeout", totalTimeout).
		Msg("Preparing batch insert with calculated timeout")

	// Prepare batch insert
	batchInsert, err := w.conn.PrepareBatch(ctx, "INSERT INTO xrpl.money_flow")
	if err != nil {
		logger.Log.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to prepare batch insert")
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	// Group rows by ledger index for detailed logging
	ledgerRowsMap := make(map[uint32][]MoneyFlowRow)
	for _, row := range batch {
		ledgerRowsMap[row.LedgerIndex] = append(ledgerRowsMap[row.LedgerIndex], row)
	}

	// Add rows to batch
	// Track failed rows to continue processing even if some rows fail
	failedRows := 0
	for _, row := range batch {
		// Convert close_time_unix to DateTime64
		closeTime := time.Unix(row.CloseTimeUnix, 0).UTC()

		// For Enum8 in ClickHouse, we need to pass the string value directly
		// ClickHouse will convert it to the corresponding enum value
		kindValue := row.Kind
		if kindValue == "" {
			kindValue = "unknown"
		}

		// Normalize Decimal strings to fit ClickHouse Decimal(38,18) format
		// This prevents "math/big: buffer too small" errors
		fromAmount := normalizeDecimalForClickHouse(row.FromAmount)
		toAmount := normalizeDecimalForClickHouse(row.ToAmount)
		initFromAmount := normalizeDecimalForClickHouse(row.InitFromAmount)
		initToAmount := normalizeDecimalForClickHouse(row.InitToAmount)
		quote := normalizeDecimalForClickHouse(row.Quote)

		// Wrap Append in panic recovery to handle individual row failures
		appendErr := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic during Append: %v", r)
					logger.Log.Error().
						Interface("panic", r).
						Str("tx_hash", row.TxHash).
						Str("from_amount", fromAmount).
						Str("to_amount", toAmount).
						Str("init_from_amount", initFromAmount).
						Str("init_to_amount", initToAmount).
						Str("quote", quote).
						Msg("Panic occurred while appending row to batch")
				}
			}()

			return batchInsert.Append(
				row.TxHash,
				row.LedgerIndex,
				row.InLedgerIndex,
				closeTime,
				row.FeeDrops,
				row.FromAddress,
				row.ToAddress,
				row.FromCurrency,
				row.FromIssuerAddress,
				row.ToCurrency,
				row.ToIssuerAddress,
				fromAmount,     // Decimal(38,18) - normalized string
				toAmount,       // Decimal(38,18) - normalized string
				initFromAmount, // Decimal(38,18) - normalized string
				initToAmount,   // Decimal(38,18) - normalized string
				quote,          // Decimal(38,18) - normalized string
				kindValue,      // Enum8 - string value is accepted
				row.Version,
			)
		}()

		if appendErr != nil {
			failedRows++
			logger.Log.Error().
				Err(appendErr).
				Str("tx_hash", row.TxHash).
				Str("from_amount", fromAmount).
				Str("to_amount", toAmount).
				Msg("Failed to append row to batch, continuing with other rows")
			// Continue processing other rows instead of returning immediately
			// This allows the batch to be sent even if some rows fail
		}
	}

	// If all rows failed, return error
	if failedRows == len(batch) {
		return fmt.Errorf("all %d rows failed to append to batch", len(batch))
	}

	// Log warning if some rows failed
	if failedRows > 0 {
		logger.Log.Warn().
			Int("failed_rows", failedRows).
			Int("total_rows", len(batch)).
			Msg("Some rows failed to append to batch, but continuing with batch send")
	}

	// Execute batch insert with retry logic
	maxRetries := 3
	baseDelay := 1 * time.Second
	maxDelay := 10 * time.Second

	var lastErr error
	var retryBatchInsert driver.Batch = batchInsert

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Calculate delay with exponential backoff
			delay := baseDelay * time.Duration(1<<uint(attempt-1))
			if delay > maxDelay {
				delay = maxDelay
			}

			logger.Log.Warn().
				Err(lastErr).
				Int("batch_size", len(batch)).
				Int("attempt", attempt+1).
				Int("max_retries", maxRetries).
				Dur("retry_in", delay).
				Msg("ClickHouse batch insert failed, retrying")

			// Wait before retry
			time.Sleep(delay)

			// Recreate context for retry with fresh timeout
			retryCtx, retryCancel := context.WithTimeout(context.Background(), totalTimeout)
			defer retryCancel()

			// Re-prepare batch for retry
			var prepareErr error
			retryBatchInsert, prepareErr = w.conn.PrepareBatch(retryCtx, "INSERT INTO xrpl.money_flow")
			if prepareErr != nil {
				lastErr = fmt.Errorf("failed to prepare batch for retry: %w", prepareErr)
				logger.Log.Error().Err(lastErr).Int("attempt", attempt+1).Msg("Failed to prepare batch for retry")
				continue
			}

			// Re-add all rows to the new batch
			appendFailed := false
			for _, row := range batch {
				closeTime := time.Unix(row.CloseTimeUnix, 0).UTC()
				kindValue := row.Kind
				if kindValue == "" {
					kindValue = "unknown"
				}
				fromAmount := normalizeDecimalForClickHouse(row.FromAmount)
				toAmount := normalizeDecimalForClickHouse(row.ToAmount)
				initFromAmount := normalizeDecimalForClickHouse(row.InitFromAmount)
				initToAmount := normalizeDecimalForClickHouse(row.InitToAmount)
				quote := normalizeDecimalForClickHouse(row.Quote)

				if appendErr := retryBatchInsert.Append(
					row.TxHash, row.LedgerIndex, row.InLedgerIndex, closeTime,
					row.FeeDrops, row.FromAddress, row.ToAddress,
					row.FromCurrency, row.FromIssuerAddress,
					row.ToCurrency, row.ToIssuerAddress,
					fromAmount, toAmount, initFromAmount, initToAmount, quote,
					kindValue, row.Version,
				); appendErr != nil {
					lastErr = fmt.Errorf("failed to append row to retry batch: %w", appendErr)
					logger.Log.Error().Err(appendErr).Str("tx_hash", row.TxHash).Msg("Failed to append row to retry batch")
					appendFailed = true
					break
				}
			}

			if appendFailed {
				continue
			}
		}

		// Try to send batch
		sendErr := retryBatchInsert.Send()
		if sendErr == nil {
			// Success!
			if attempt > 0 {
				logger.Log.Info().
					Int("batch_size", len(batch)).
					Int("attempt", attempt+1).
					Msg("ClickHouse batch insert succeeded after retry")
			}
			lastErr = nil
			break
		}

		lastErr = sendErr

		// Check if error is retryable
		if !IsRetryableError(sendErr) {
			logger.Log.Error().
				Err(sendErr).
				Int("batch_size", len(batch)).
				Msg("ClickHouse batch insert failed with non-retryable error")
			// For non-retryable errors, return immediately but don't crash
			// Put batch back into queue for potential later retry
			w.mu.Lock()
			// Prepend failed batch back to the beginning (FIFO order)
			w.moneyFlowBatch = append(batch, w.moneyFlowBatch...)
			w.mu.Unlock()
			return fmt.Errorf("non-retryable error during batch insert: %w", sendErr)
		}

		// If this was the last attempt, handle failure gracefully
		if attempt == maxRetries-1 {
			logger.Log.Error().
				Err(sendErr).
				Int("batch_size", len(batch)).
				Int("max_retries", maxRetries).
				Msg("ClickHouse batch insert failed after all retries - putting batch back in queue")

			// Put batch back into queue for potential later retry
			// This prevents data loss and allows the system to recover
			w.mu.Lock()
			w.moneyFlowBatch = append(batch, w.moneyFlowBatch...)
			w.mu.Unlock()

			return fmt.Errorf("failed to send batch after %d retries: %w", maxRetries, sendErr)
		}
	}

	// Check for panic error first
	if panicErr != nil {
		// Put batch back in queue after panic
		w.mu.Lock()
		w.moneyFlowBatch = append(batch, w.moneyFlowBatch...)
		w.mu.Unlock()
		return panicErr
	}

	// If we still have an error after all retries, handle it
	if lastErr != nil {
		logger.Log.Error().
			Err(lastErr).
			Int("batch_size", len(batch)).
			Msg("ClickHouse batch insert failed after retries")
		// Put batch back in queue
		w.mu.Lock()
		w.moneyFlowBatch = append(batch, w.moneyFlowBatch...)
		w.mu.Unlock()
		return fmt.Errorf("failed to send batch: %w", lastErr)
	}

	// Log summary of written rows grouped by ledger index
	ledgerSummary := make(map[uint32]int)
	for _, row := range batch {
		ledgerSummary[row.LedgerIndex]++
	}

	logger.Log.Info().
		Int("batch_size", len(batch)).
		Interface("ledgers", ledgerSummary).
		Msg("Successfully flushed batch to ClickHouse")

	// Call flush callback if set (e.g., for token detection)
	// Run in goroutine to not block the flush operation
	if w.flushCallback != nil {
		go func() {
			// Make a copy of the batch to avoid race conditions
			batchCopy := make([]MoneyFlowRow, len(batch))
			copy(batchCopy, batch)
			w.flushCallback(batchCopy)
		}()
	}

	// Check for subscription activities after successful flush
	// Run in goroutine to not block the flush operation
	go checkAndEmitSubscriptionActivities(batch)

	// Detailed logging for specific ledger indices
	for ledgerIndex, rows := range ledgerRowsMap {
		if config.ShouldLogDetailed(ledgerIndex) {
			logger.Log.Info().
				Uint32("ledger_index", ledgerIndex).
				Int("rows_count", len(rows)).
				Msg("=== Detailed logging for ledger ===")

			for i, row := range rows {
				logger.Log.Info().
					Uint32("ledger_index", ledgerIndex).
					Int("row_number", i+1).
					Str("tx_hash", row.TxHash).
					Uint32("in_ledger_index", row.InLedgerIndex).
					Int64("close_time_unix", row.CloseTimeUnix).
					Uint64("fee_drops", row.FeeDrops).
					Str("from_address", row.FromAddress).
					Str("to_address", row.ToAddress).
					Str("from_currency", row.FromCurrency).
					Str("from_issuer_address", row.FromIssuerAddress).
					Str("to_currency", row.ToCurrency).
					Str("to_issuer_address", row.ToIssuerAddress).
					Str("from_amount", row.FromAmount).
					Str("to_amount", row.ToAmount).
					Str("init_from_amount", row.InitFromAmount).
					Str("init_to_amount", row.InitToAmount).
					Str("quote", row.Quote).
					Str("kind", row.Kind).
					Uint64("version", row.Version).
					Msg("Money flow row written to database")
			}

			logger.Log.Info().
				Uint32("ledger_index", ledgerIndex).
				Int("total_rows", len(rows)).
				Msg("=== End of detailed logging for ledger ===")
		}
	}

	// Check if panic occurred and return error instead of panicking
	if panicErr != nil {
		return panicErr
	}

	return nil
}

// normalizeDecimalForClickHouse normalizes a decimal string to fit ClickHouse Decimal(38,18) format
// ClickHouse Decimal(38,18) means: up to 38 digits total, with 18 digits after decimal point
// This function ensures the value fits within these constraints to prevent "buffer too small" errors
func normalizeDecimalForClickHouse(decimalStr string) string {
	if decimalStr == "" {
		return "0"
	}

	// Remove leading/trailing whitespace
	decimalStr = strings.TrimSpace(decimalStr)

	// Handle infinity values (API returns "+Inf" when there's no data)
	// Convert to "0" as ClickHouse Decimal cannot store infinity
	upperStr := strings.ToUpper(decimalStr)
	if upperStr == "+INF" || upperStr == "-INF" || upperStr == "INF" || upperStr == "INFINITY" || upperStr == "+INFINITY" || upperStr == "-INFINITY" {
		return "0"
	}

	// Handle zero
	if decimalStr == "0" || decimalStr == "0.0" || decimalStr == "0.00" {
		return "0"
	}

	// Parse the decimal string
	// Split by decimal point
	parts := strings.Split(decimalStr, ".")
	intPart := parts[0]
	fracPart := ""
	if len(parts) > 1 {
		fracPart = parts[1]
	}

	// Handle negative sign
	negative := false
	if strings.HasPrefix(intPart, "-") {
		negative = true
		intPart = intPart[1:]
	}

	// Remove leading zeros from integer part (but keep at least one digit)
	intPart = strings.TrimLeft(intPart, "0")
	if intPart == "" {
		intPart = "0"
	}

	// Limit integer part to 38 digits (but we need to leave room for fractional part)
	// Decimal(38,18) means: 38 total digits, 18 after decimal point
	// So max integer part is 38 - 18 = 20 digits
	maxIntDigits := 20
	if len(intPart) > maxIntDigits {
		// Truncate integer part (this is a data issue, but we need to handle it)
		intPart = intPart[:maxIntDigits]
		logger.Log.Warn().
			Str("original", decimalStr).
			Str("truncated_int", intPart).
			Msg("Decimal integer part too large, truncated to fit ClickHouse Decimal(38,18)")
	}

	// Limit fractional part to 18 digits
	maxFracDigits := 18
	if len(fracPart) > maxFracDigits {
		// Round or truncate fractional part
		fracPart = fracPart[:maxFracDigits]
	}

	// Remove trailing zeros from fractional part
	fracPart = strings.TrimRight(fracPart, "0")

	// Reconstruct the decimal string
	result := intPart
	if negative {
		result = "-" + result
	}
	if fracPart != "" {
		result = result + "." + fracPart
	}

	return result
}

// WriteMoneyFlowRow writes a money flow row (convenience wrapper)
// Accepts all fields as parameters to avoid circular import with models package
func WriteMoneyFlowRow(
	txHash string,
	ledgerIndex uint32,
	inLedgerIndex uint32,
	closeTimeUnix int64,
	feeDrops uint64,
	fromAddress string,
	toAddress string,
	fromCurrency string,
	fromIssuerAddress string,
	toCurrency string,
	toIssuerAddress string,
	fromAmount string,
	toAmount string,
	initFromAmount string,
	initToAmount string,
	quote string,
	kind string,
	version uint64,
) error {
	if chBatchWriter == nil {
		return fmt.Errorf("ClickHouse batch writer not initialized")
	}

	moneyFlowRow := MoneyFlowRow{
		TxHash:            txHash,
		LedgerIndex:       ledgerIndex,
		InLedgerIndex:     inLedgerIndex,
		CloseTimeUnix:     closeTimeUnix,
		FeeDrops:          feeDrops,
		FromAddress:       fromAddress,
		ToAddress:         toAddress,
		FromCurrency:      fromCurrency,
		FromIssuerAddress: fromIssuerAddress,
		ToCurrency:        toCurrency,
		ToIssuerAddress:   toIssuerAddress,
		FromAmount:        fromAmount,
		ToAmount:          toAmount,
		InitFromAmount:    initFromAmount,
		InitToAmount:      initToAmount,
		Quote:             quote,
		Kind:              kind,
		Version:           version,
	}

	// Only log trace info if detailed logging is enabled for this ledger
	if config.ShouldLogDetailed(ledgerIndex) {
		logger.Log.Trace().
			Str("tx_hash", txHash).
			Uint32("ledger_index", ledgerIndex).
			Str("kind", kind).
			Str("from_address", fromAddress).
			Str("to_address", toAddress).
			Msg("Adding money flow row to ClickHouse batch")
	}

	return chBatchWriter.WriteMoneyFlow(moneyFlowRow)
}

// FlushClickHouse flushes all pending batches
func FlushClickHouse() error {
	if chBatchWriter == nil {
		return nil
	}
	chBatchWriter.mu.Lock()
	pendingCount := len(chBatchWriter.moneyFlowBatch)
	chBatchWriter.mu.Unlock()

	if pendingCount > 0 {
		logger.Log.Info().Int("pending_rows", pendingCount).Msg("Flushing pending ClickHouse batches")
	} else {
		logger.Log.Info().Msg("No pending ClickHouse batches to flush")
	}

	return chBatchWriter.Flush()
}

var closeClickHouseOnce sync.Once

// CloseClickHouse closes ClickHouse connection
func CloseClickHouse() {
	closeClickHouseOnce.Do(func() {
		if chBatchWriter != nil {
			// Flush all pending batches before stopping
			log.Println("Flushing all pending ClickHouse batches...")
			if err := chBatchWriter.Flush(); err != nil {
				log.Printf("Error flushing ClickHouse batches: %v", err)
			}

			log.Println("Stopping ClickHouse batch writer...")
			chBatchWriter.Stop()
			log.Println("ClickHouse batch writer stopped")
		}

		if ClickHouseConn != nil {
			log.Println("Closing ClickHouse connection...")
			if err := ClickHouseConn.Close(); err != nil {
				log.Printf("Error closing ClickHouse connection: %v", err)
			} else {
				log.Println("ClickHouse connection closed")
			}
		}
	})
}

// WriteLedgerToClickHouse writes ledger data directly to ClickHouse (if needed in future)
func WriteLedgerToClickHouse(ledgerJSON []byte) error {
	// This is a placeholder for future direct ledger writes
	logger.Log.Debug().Msg("WriteLedgerToClickHouse called (not implemented)")
	return nil
}

// WriteTransactionToClickHouse writes transaction data directly to ClickHouse (if needed in future)
func WriteTransactionToClickHouse(txJSON []byte) error {
	// Transactions are processed by consumers and converted to money flows
	// Direct transaction writes are not needed as they go through the consumer pipeline
	logger.Log.Debug().Msg("WriteTransactionToClickHouse called (not implemented)")
	return nil
}

// RecordEmptyLedger records a ledger that has no Payment transactions
func RecordEmptyLedger(ledgerIndex uint32, closeTimeUnix int64, totalTransactions uint32) error {
	if ClickHouseConn == nil {
		return fmt.Errorf("ClickHouse connection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Convert close_time from Ripple epoch to Unix timestamp
	const rippleToUnix int64 = 946684800
	closeTime := time.Unix(closeTimeUnix+rippleToUnix, 0).UTC()

	query := `
		INSERT INTO xrpl.empty_ledgers 
		(ledger_index, close_time, total_transactions, checked_at, version)
		VALUES (?, ?, ?, now64(), now64())
	`

	maxRetries := 3
	baseDelay := 500 * time.Millisecond
	maxDelay := 5 * time.Second
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			delay := baseDelay * time.Duration(1<<uint(attempt-1))
			if delay > maxDelay {
				delay = maxDelay
			}
			logger.Log.Warn().
				Err(lastErr).
				Uint32("ledger_index", ledgerIndex).
				Int("attempt", attempt+1).
				Dur("retry_in", delay).
				Msg("Retrying empty_ledgers insert")
			time.Sleep(delay)

			// Recreate context for retry
			ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
		}

		lastErr = ClickHouseConn.Exec(ctx, query,
			ledgerIndex,
			closeTime,
			totalTransactions,
		)

		if lastErr == nil {
			if attempt > 0 {
				logger.Log.Info().
					Uint32("ledger_index", ledgerIndex).
					Int("attempt", attempt+1).
					Msg("Successfully inserted empty ledger after retry")
			}
			return nil
		}

		// Check if error is retryable
		if !IsRetryableError(lastErr) {
			logger.Log.Error().
				Err(lastErr).
				Uint32("ledger_index", ledgerIndex).
				Msg("Non-retryable error inserting empty ledger")
			return fmt.Errorf("failed to insert empty ledger (non-retryable): %w", lastErr)
		}

		if attempt == maxRetries-1 {
			logger.Log.Error().
				Err(lastErr).
				Uint32("ledger_index", ledgerIndex).
				Int("max_retries", maxRetries).
				Msg("Failed to insert empty ledger after all retries")
			return fmt.Errorf("failed to insert empty ledger after %d retries: %w", maxRetries, lastErr)
		}
	}

	return fmt.Errorf("failed to insert empty ledger: %w", lastErr)
}

// checkAndEmitSubscriptionActivities checks for activities on subscribed addresses
// and emits SocketIO events to subscribers
func checkAndEmitSubscriptionActivities(batch []MoneyFlowRow) {
	if ClickHouseConn == nil {
		logger.Log.Warn().Msg("checkAndEmitSubscriptionActivities: ClickHouse connection not initialized")
		return
	}

	// Filter batch for swap or dexOffer activities (as per original requirement)
	// But also log all kinds for debugging
	relevantRows := make([]MoneyFlowRow, 0)
	kindCounts := make(map[string]int)
	for _, row := range batch {
		kindCounts[row.Kind]++
		if row.Kind == "swap" || row.Kind == "dexOffer" {
			relevantRows = append(relevantRows, row)
		}
	}

	// Collect unique addresses (both from and to)
	addressSet := make(map[string]bool)
	for _, row := range relevantRows {
		if row.FromAddress != "" {
			addressSet[row.FromAddress] = true
		}
		if row.ToAddress != "" {
			addressSet[row.ToAddress] = true
		}
	}

	// Build list of addresses for SQL IN clause
	addresses := make([]string, 0, len(addressSet))
	for addr := range addressSet {
		addresses = append(addresses, addr)
	}

	// Query subscription_links to find subscribers
	// We need to find all subscription_links where to_address is in our list
	queryCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// For ClickHouse, build query with multiple ? placeholders for IN clause
	if len(addresses) == 0 {
		return
	}

	// Build query with placeholders for addresses
	placeholders := make([]string, len(addresses))
	args := make([]interface{}, len(addresses))
	for i, addr := range addresses {
		placeholders[i] = "?"
		args[i] = addr
	}

	// Note: subscription_links uses MergeTree, not ReplacingMergeTree, so FINAL is not needed
	query := fmt.Sprintf(`
		SELECT DISTINCT from_address, to_address
		FROM xrpl.subscription_links
		WHERE to_address IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := ClickHouseConn.Query(queryCtx, query, args...)
	if err != nil {
		logger.Log.Error().
			Err(err).
			Str("query", query).
			Msg("checkAndEmitSubscriptionActivities: failed to query subscription_links")
		return
	}
	defer rows.Close()

	// Build map: subscribed_address -> list of subscriber_addresses
	subscriptionMap := make(map[string][]string)
	subscriptionCount := 0
	for rows.Next() {
		var subscriberAddr, subscribedAddr string
		if err := rows.Scan(&subscriberAddr, &subscribedAddr); err != nil {
			logger.Log.Error().Err(err).Msg("checkAndEmitSubscriptionActivities: failed to scan subscription row")
			continue
		}
		subscriptionMap[subscribedAddr] = append(subscriptionMap[subscribedAddr], subscriberAddr)
		subscriptionCount++
	}

	// Group transactions by address and collect subscribers
	// Map: tx_hash -> subscribers list
	transactionSubscribers := make(map[string][]string)
	transactionData := make(map[string]MoneyFlowRow)

	for _, row := range relevantRows {
		// Check both from_address and to_address
		addressesToCheck := []string{}
		if row.FromAddress != "" {
			addressesToCheck = append(addressesToCheck, row.FromAddress)
		}
		if row.ToAddress != "" && row.ToAddress != row.FromAddress {
			addressesToCheck = append(addressesToCheck, row.ToAddress)
		}

		// Collect all subscribers for this transaction
		allSubscribers := make(map[string]bool)
		for _, addr := range addressesToCheck {
			subscribers, exists := subscriptionMap[addr]
			if exists {
				for _, subscriber := range subscribers {
					allSubscribers[subscriber] = true
				}
			}
		}

		// If there are subscribers, add this transaction
		if len(allSubscribers) > 0 {
			subscribersList := make([]string, 0, len(allSubscribers))
			for subscriber := range allSubscribers {
				subscribersList = append(subscribersList, subscriber)
			}
			transactionSubscribers[row.TxHash] = subscribersList
			transactionData[row.TxHash] = row
		}
	}

	// If no transactions with subscribers, return early
	if len(transactionSubscribers) == 0 {
		logger.Log.Debug().
			Int("relevant_rows", len(relevantRows)).
			Int("subscribed_addresses", len(subscriptionMap)).
			Msg("checkAndEmitSubscriptionActivities: no events emitted (no matching subscriptions)")
		return
	}

	// Build activities array
	activities := make([]socketio.SubscriptionActivityItem, 0, len(transactionSubscribers))
	for txHash, subscribers := range transactionSubscribers {
		row := transactionData[txHash]
		activities = append(activities, socketio.SubscriptionActivityItem{
			Subscribers: subscribers,
			MoneyFlow: socketio.MoneyFlowData{
				TxHash:       row.TxHash,
				LedgerIndex:  row.LedgerIndex,
				Kind:         row.Kind,
				FromAddress:  row.FromAddress,
				ToAddress:    row.ToAddress,
				FromCurrency: row.FromCurrency,
				FromIssuer:   row.FromIssuerAddress,
				ToCurrency:   row.ToCurrency,
				ToIssuer:     row.ToIssuerAddress,
				FromAmount:   row.FromAmount,
				ToAmount:     row.ToAmount,
				Timestamp:    row.CloseTimeUnix,
			},
		})
	}

	// Emit single event with all activities
	hub := socketio.GetHub()
	event := socketio.SubscriptionActivityEvent{
		Activities: activities,
	}

	hub.EmitSubscriptionActivity(event)
}

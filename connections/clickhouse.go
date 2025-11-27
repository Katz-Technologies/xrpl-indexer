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
		chBatchWriter = NewClickHouseBatchWriter(conn, batchSize, time.Duration(batchTimeoutMs)*time.Millisecond)
		chBatchWriter.Start()
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
				w.Flush()
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
		logger.Log.Debug().Int("batch_size", currentBatchSize).Msg("Batch is full, flushing to ClickHouse")
		return w.flushUnlocked()
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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

	// Execute batch insert
	if err := batchInsert.Send(); err != nil {
		logger.Log.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to send batch insert")
		return fmt.Errorf("failed to send batch: %w", err)
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

	err := ClickHouseConn.Exec(ctx, query,
		ledgerIndex,
		closeTime,
		totalTransactions,
	)

	if err != nil {
		return fmt.Errorf("failed to insert empty ledger: %w", err)
	}

	return nil
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

	logger.Log.Debug().
		Str("query", query).
		Int("address_count", len(addresses)).
		Msg("checkAndEmitSubscriptionActivities: executing subscription query")

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

	logger.Log.Debug().
		Int("subscriptions_found", subscriptionCount).
		Int("unique_subscribed_addresses", len(subscriptionMap)).
		Msg("checkAndEmitSubscriptionActivities: subscription query results")

	if len(subscriptionMap) == 0 {
		logger.Log.Debug().
			Strs("checked_addresses", addresses[:min(10, len(addresses))]).
			Msg("checkAndEmitSubscriptionActivities: no subscriptions found for any addresses")
		return
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

	logger.Log.Debug().
		Int("transactions_count", len(activities)).
		Int("total_subscribers", countTotalSubscribers(transactionSubscribers)).
		Msg("checkAndEmitSubscriptionActivities: emitting batch event")

	hub.EmitSubscriptionActivity(event)

	logger.Log.Info().
		Int("transactions_count", len(activities)).
		Int("total_subscribers", countTotalSubscribers(transactionSubscribers)).
		Int("relevant_rows", len(relevantRows)).
		Msg("Emitted subscription activity batch event")
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// countTotalSubscribers counts total unique subscribers across all transactions
func countTotalSubscribers(transactionSubscribers map[string][]string) int {
	uniqueSubscribers := make(map[string]bool)
	for _, subscribers := range transactionSubscribers {
		for _, subscriber := range subscribers {
			uniqueSubscribers[subscriber] = true
		}
	}
	return len(uniqueSubscribers)
}

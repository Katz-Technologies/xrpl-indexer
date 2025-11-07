package connections

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/logger"
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
	close(w.stopChan)
	if w.flushTicker != nil {
		w.flushTicker.Stop()
	}
	w.wg.Wait()
	w.Flush() // Final flush
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
	defer w.mu.Unlock()
	return w.flushUnlocked()
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Prepare batch insert
	batchInsert, err := w.conn.PrepareBatch(ctx, "INSERT INTO xrpl.money_flow")
	if err != nil {
		logger.Log.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to prepare batch insert")
		w.mu.Lock()
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

		// Add rows to batch
		for _, row := range batch {
			// Convert close_time_unix to DateTime64
			closeTime := time.Unix(row.CloseTimeUnix, 0).UTC()

			// For Enum8 in ClickHouse, we need to pass the string value directly
			// ClickHouse will convert it to the corresponding enum value
			kindValue := row.Kind
			if kindValue == "" {
				kindValue = "unknown"
			}

			// Convert Decimal strings to proper Decimal types
			// clickhouse-go accepts strings for Decimal and converts them automatically
			// But we can also use decimal.Decimal if needed, for now strings should work
			err := batchInsert.Append(
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
				row.FromAmount,        // Decimal(38,18) - string is accepted
				row.ToAmount,          // Decimal(38,18) - string is accepted
				row.InitFromAmount,    // Decimal(38,18) - string is accepted
				row.InitToAmount,      // Decimal(38,18) - string is accepted
				row.Quote,             // Decimal(38,18) - string is accepted
				kindValue,             // Enum8 - string value is accepted
				row.Version,
			)
			if err != nil {
				logger.Log.Error().Err(err).Str("tx_hash", row.TxHash).Msg("Failed to append row to batch")
				w.mu.Lock()
				return fmt.Errorf("failed to append row: %w", err)
			}
		}

	// Execute batch insert
	if err := batchInsert.Send(); err != nil {
		logger.Log.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to send batch insert")
		w.mu.Lock()
		return fmt.Errorf("failed to send batch: %w", err)
	}

	logger.Log.Info().Int("batch_size", len(batch)).Msg("Successfully flushed batch to ClickHouse")
	w.mu.Lock()
	return nil
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

// CloseClickHouse closes ClickHouse connection
func CloseClickHouse() {
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
}

// WriteLedgerToClickHouse writes ledger data directly to ClickHouse (if needed in future)
// Currently ledgers are written to Kafka, but this can be used for direct writes
func WriteLedgerToClickHouse(ledgerJSON []byte) error {
	// This is a placeholder for future direct ledger writes
	// For now, we keep ledger writes to Kafka as they are processed by consumers
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


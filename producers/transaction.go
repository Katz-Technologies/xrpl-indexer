package producers

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/consumers"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/models"
	"github.com/xrpscan/xrpl-go"
)

// ProcessTransactionsDirectly processes transactions from a ledger and writes directly to ClickHouse
// isRealtime indicates if this is a real-time ledger (true) or backfill (false)
// SocketIO events are only emitted for real-time transactions
func ProcessTransactionsDirectly(message []byte, isRealtime bool) {
	var ledger models.LedgerStream
	if err := json.Unmarshal(message, &ledger); err != nil {
		logger.Log.Error().Err(err).Msg("JSON Unmarshal error")
		return
	}

	// Fetch all transactions included in this ledger from XRPL server
	txResponse, err := ledger.FetchTransactions()
	if err != nil {
		logger.Log.Error().
			Uint32("ledger_index", ledger.LedgerIndex).
			Err(err).
			Msg("Failed to fetch transactions after retries, skipping ledger")
		return
	}

	// Verify if result.ledger.transactions property is present
	txResult, ok := txResponse["result"].(map[string]interface{})
	if !ok {
		logger.Log.Error().Uint32("ledger_index", ledger.LedgerIndex).Msg("Tx response has no result property")
		return
	}
	txLedger, ok := txResult["ledger"].(map[string]interface{})
	if !ok {
		logger.Log.Error().Uint32("ledger_index", ledger.LedgerIndex).Msg("Tx response has no result.ledger property")
		return
	}
	txs, ok := txLedger["transactions"].([]interface{})
	if !ok {
		logger.Log.Error().Uint32("ledger_index", ledger.LedgerIndex).Msg("Tx response has no result.ledger.transactions property")
		return
	}

	// Type assert ledger_index and date fields
	ledgerIndexStr, ok := txLedger["ledger_index"].(string)
	if !ok {
		logger.Log.Error().Uint32("ledger_index", ledger.LedgerIndex).Msg("Ledger has invalid ledger_index property")
		return
	}
	ledgerIndex, err := strconv.Atoi(ledgerIndexStr)
	if err != nil {
		logger.Log.Error().Uint32("ledger_index", ledger.LedgerIndex).Msg("Cannot convert ledger_index to int")
		return
	}
	closeTime, ok := txLedger["close_time"].(float64)
	if !ok {
		logger.Log.Error().Uint32("ledger_index", ledger.LedgerIndex).Msg("Ledger has invalid close_time property")
		return
	}

	// Convert closeTime to time.Time for token checking
	const rippleToUnix int64 = 946684800
	ledgerTimestamp := time.Unix(int64(closeTime)+rippleToUnix, 0).UTC()

	// Check for new tokens (only for real-time ledgers to avoid API spam during backfill)
	// This runs asynchronously in a separate goroutine to not block transaction processing
	if isRealtime {
		// Use background context with longer timeout for token checking
		// This allows retries to complete even if they take longer
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

		// Prepare transactions for token checking (need to populate ledger_index and date first)
		// IMPORTANT: Copy transactions before passing to goroutine to avoid concurrent map access
		preparedTxs := make([]interface{}, 0, len(txs))
		for _, txo := range txs {
			tx, ok := txo.(map[string]interface{})
			if !ok {
				continue
			}
			// Populate required fields
			tx["ledger_index"] = float64(ledgerIndex)
			tx["date"] = closeTime
			tx["validated"] = true
			preparedTxs = append(preparedTxs, tx)
		}

		// Deep copy transactions before passing to goroutine to avoid concurrent map access
		copiedTxs := connections.DeepCopyTransactions(preparedTxs)

		// Check for new tokens asynchronously to not block transaction processing
		// This goroutine runs independently and won't block the main indexing flow
		go func() {
			defer cancel() // Ensure context is cancelled when done
			if err := connections.CheckAndNotifyNewTokens(ctx, copiedTxs, ledger.LedgerIndex, ledgerTimestamp); err != nil {
				logger.Log.Warn().
					Err(err).
					Uint32("ledger_index", ledger.LedgerIndex).
					Msg("Error checking for new tokens (non-blocking)")
			}
		}()
	}

	// Process transactions directly and write to ClickHouse
	for _, txo := range txs {
		tx, ok := txo.(map[string]interface{})
		if !ok {
			logger.Log.Error().Uint32("ledger_index", ledger.LedgerIndex).Msg("Error asserting transaction type")
			continue
		}

		// Transactions fetched by `ledger` command do not have date, validated,
		// ledger_index fields. Populating these tx fields from ledger data.
		// Use float64 for ledger_index to match XRPL format
		tx["ledger_index"] = float64(ledgerIndex)
		tx["date"] = closeTime
		tx["validated"] = true

		var base xrpl.BaseResponse = tx
		hash, _ := base["hash"].(string)
		if hash == "" {
			// Skip malformed tx
			continue
		}

		// Process transaction directly and write to ClickHouse
		_, err := consumers.ProcessTransaction(tx)
		if err != nil {
			logger.Log.Error().Err(err).Str("tx_hash", hash).Uint32("ledger_index", ledger.LedgerIndex).Msg("Failed to process transaction")
			// Continue processing other transactions even if one fails
		} 
	}
}

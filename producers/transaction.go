package producers

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"time"

	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/consumers"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/models"
	"github.com/xrpscan/xrpl-go"
)

// Helper function to get map keys for debugging
func getMapKeys(m map[string]interface{}) []string {
	if m == nil {
		return []string{}
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Helper function to get type name for debugging
func getTypeName(v interface{}) string {
	if v == nil {
		return "nil"
	}
	return reflect.TypeOf(v).String()
}

// ProcessTransactionsDirectly processes transactions from a ledger and writes directly to ClickHouse
// isRealtime indicates if this is a real-time ledger (true) or backfill (false)
// SocketIO events are only emitted for real-time transactions
func ProcessTransactionsDirectly(message []byte, isRealtime bool) {
	var ledger models.LedgerStream
	if err := json.Unmarshal(message, &ledger); err != nil {
		logger.Log.Error().Err(err).Msg("JSON Unmarshal error")
		return
	}

	logger.Log.Info().
		Uint32("ledger_index", ledger.LedgerIndex).
		Bool("is_realtime", isRealtime).
		Msg("Starting to process ledger transactions")

	// Fetch all transactions included in this ledger from XRPL server
	txResponse, err := ledger.FetchTransactions()
	if err != nil {
		logger.Log.Error().
			Uint32("ledger_index", ledger.LedgerIndex).
			Err(err).
			Msg("Failed to fetch transactions after retries, skipping ledger")
		return
	}

	logger.Log.Info().
		Uint32("ledger_index", ledger.LedgerIndex).
		Msg("Successfully fetched transactions from XRPL server")

	// Verify if result.ledger.transactions property is present
	txResult, ok := txResponse["result"].(map[string]interface{})
	if !ok {
		// Log full response for debugging
		logger.Log.Error().
			Uint32("ledger_index", ledger.LedgerIndex).
			Interface("response_keys", getMapKeys(txResponse)).
			Interface("response_status", txResponse["status"]).
			Interface("response_error", txResponse["error"]).
			Msg("Tx response has no result property - logging response structure")
		return
	}
	txLedger, ok := txResult["ledger"].(map[string]interface{})
	if !ok {
		logger.Log.Error().
			Uint32("ledger_index", ledger.LedgerIndex).
			Interface("result_keys", getMapKeys(txResult)).
			Msg("Tx response has no result.ledger property - logging result structure")
		return
	}
	txs, ok := txLedger["transactions"].([]interface{})
	if !ok {
		logger.Log.Error().
			Uint32("ledger_index", ledger.LedgerIndex).
			Interface("ledger_keys", getMapKeys(txLedger)).
			Msg("Tx response has no result.ledger.transactions property - logging ledger structure")
		return
	}

	// Type assert ledger_index and date fields
	ledgerIndexStr, ok := txLedger["ledger_index"].(string)
	if !ok {
		logger.Log.Error().
			Uint32("ledger_index", ledger.LedgerIndex).
			Interface("ledger_index_value", txLedger["ledger_index"]).
			Interface("ledger_index_type", getTypeName(txLedger["ledger_index"])).
			Msg("Ledger has invalid ledger_index property - expected string")
		return
	}
	ledgerIndex, err := strconv.Atoi(ledgerIndexStr)
	if err != nil {
		logger.Log.Error().
			Uint32("ledger_index", ledger.LedgerIndex).
			Str("ledger_index_str", ledgerIndexStr).
			Err(err).
			Msg("Cannot convert ledger_index to int")
		return
	}

	// Try to get close_time with flexible type handling
	var closeTime float64
	closeTimeFound := false

	// Try float64 first (most common format)
	if ct, ok := txLedger["close_time"].(float64); ok {
		closeTime = ct
		closeTimeFound = true
	} else if ct, ok := txLedger["close_time"].(uint32); ok {
		// Try uint32 (Ripple epoch format)
		closeTime = float64(ct)
		closeTimeFound = true
	} else if ct, ok := txLedger["close_time"].(int64); ok {
		// Try int64
		closeTime = float64(ct)
		closeTimeFound = true
	} else if ct, ok := txLedger["close_time"].(int); ok {
		// Try int
		closeTime = float64(ct)
		closeTimeFound = true
	} else if ledger.LedgerTime > 0 {
		// Fallback to LedgerTime from LedgerStream (Ripple epoch)
		// Convert Ripple epoch to Unix timestamp: Ripple epoch = Unix timestamp - 946684800
		closeTime = float64(ledger.LedgerTime + 946684800)
		closeTimeFound = true
		logger.Log.Debug().
			Uint32("ledger_index", ledger.LedgerIndex).
			Uint32("ledger_time", ledger.LedgerTime).
			Msg("Using LedgerTime from LedgerStream as fallback for close_time")
	}

	if !closeTimeFound {
		logger.Log.Error().
			Uint32("ledger_index", ledger.LedgerIndex).
			Interface("close_time_value", txLedger["close_time"]).
			Interface("close_time_type", getTypeName(txLedger["close_time"])).
			Uint32("ledger_time", ledger.LedgerTime).
			Interface("ledger_keys", getMapKeys(txLedger)).
			Msg("Ledger has invalid close_time property - tried multiple formats, none worked")
		return
	}

	// Token detection is now handled in ProcessTransaction via money_flow records
	// No need to check tokens here anymore

	// Check if we're in subprocess mode (IPC mode)
	ipcProtocol := GetIPCProtocol()
	if ipcProtocol != nil {
		// Subprocess mode: send transactions via IPC to orchestrator
		logger.Log.Info().
			Uint32("ledger_index", ledger.LedgerIndex).
			Int("total_txs", len(txs)).
			Msg("Processing ledger in IPC mode")

		preparedTxs := make([]map[string]interface{}, 0, len(txs))
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

			preparedTxs = append(preparedTxs, tx)
		}

		// Send batch via IPC
		if len(preparedTxs) > 0 {
			if sent := SendBatchTransactionsViaIPC(preparedTxs); sent {
				logger.Log.Info().
					Uint32("ledger_index", ledger.LedgerIndex).
					Int("txs_sent", len(preparedTxs)).
					Msg("Sent transactions batch via IPC")
			} else {
				logger.Log.Error().
					Uint32("ledger_index", ledger.LedgerIndex).
					Int("txs_count", len(preparedTxs)).
					Msg("Failed to send transactions batch via IPC")
			}
		} else {
			logger.Log.Warn().
				Uint32("ledger_index", ledger.LedgerIndex).
				Int("total_txs_from_api", len(txs)).
				Msg("No transactions to send via IPC after filtering")
		}
	} else {
		// Standalone mode: process transactions directly and write to ClickHouse
		allMoneyFlows := make([]connections.MoneyFlowRow, 0)
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
			_, moneyFlows, err := consumers.ProcessTransaction(tx)
			if err != nil {
				logger.Log.Error().Err(err).Str("tx_hash", hash).Uint32("ledger_index", ledger.LedgerIndex).Msg("Failed to process transaction")
				// Continue processing other transactions even if one fails
			} else {
				allMoneyFlows = append(allMoneyFlows, moneyFlows...)
			}
		}

		// Check for new tokens from all payout money flows
		if len(allMoneyFlows) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			if err := connections.CheckAndNotifyNewTokens(ctx, allMoneyFlows, ledger.LedgerIndex); err != nil {
				logger.Log.Warn().
					Err(err).
					Uint32("ledger_index", ledger.LedgerIndex).
					Msg("Error checking for new tokens")
			}
		}
	}
}

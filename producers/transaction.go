package producers

import (
	"encoding/json"
	"strconv"

	"github.com/xrpscan/platform/consumers"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/models"
	"github.com/xrpscan/xrpl-go"
)

// ProcessTransactionsDirectly processes transactions from a ledger and writes directly to ClickHouse
func ProcessTransactionsDirectly(message []byte) {
	var ledger models.LedgerStream
	if err := json.Unmarshal(message, &ledger); err != nil {
		logger.Log.Error().Err(err).Msg("JSON Unmarshal error")
		return
	}

	// Fetch all transactions included in this ledger from XRPL server
	txResponse, err := ledger.FetchTransactions()
	if err != nil {
		logger.Log.Error().Uint32("ledger_index", ledger.LedgerIndex).Err(err).Msg(err.Error())
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
		if err := consumers.ProcessTransaction(tx); err != nil {
			logger.Log.Error().Err(err).Str("tx_hash", hash).Uint32("ledger_index", ledger.LedgerIndex).Msg("Failed to process transaction")
			// Continue processing other transactions even if one fails
		}
	}
}

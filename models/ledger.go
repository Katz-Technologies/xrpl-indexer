package models

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/xrpl-go"
	"github.com/xrpscan/xrpl-go/models"
)

// XRPL Genesis ledger is 32570 - https://xrpscan.com/ledger/32570
const GENESIS_LEDGER uint32 = 0

// LedgerStream type is constant 'ledgerClosed' - https://xrpl.org/subscribe.html#ledger-stream
const LEDGER_STREAM_TYPE string = "ledgerClosed"

// Ledger struct represents output of 'ledger' websocket command
// Ref: https://xrpl.org/ledger.html#response-format
type Ledger struct {
	Hash                string               `json:"hash,omitempty"`
	LedgerHash          string               `json:"ledger_hash,omitempty"`
	CloseTimeHuman      string               `json:"close_time_human,omitempty"`
	TransactionHash     string               `json:"transaction_hash,omitempty"`
	AccountHash         string               `json:"account_hash,omitempty"`
	ParentHash          string               `json:"parent_hash,omitempty"`
	Transactions        []models.Transaction `json:"transactions,omitempty"`
	Total_Coins         int64                `json:"totalCoins,omitempty"`
	TotalCoins          int64                `json:"total_coins,omitempty"`
	CloseFlags          uint32               `json:"close_flags,omitempty"`
	ParentCloseTime     uint32               `json:"parent_close_time,omitempty"`
	CloseTimeResolution uint32               `json:"close_time_resolution,omitempty"`
	SeqNum              uint32               `json:"seq_num,omitempty"`
	LedgerIndex         uint32               `json:"ledger_index,omitempty"`
	CloseTime           uint32               `json:"close_time,omitempty"`
	Closed              bool                 `json:"closed,omitempty"`
	Accepted            bool                 `json:"accepted,omitempty"`
}

// LedgerStream struct represents ledger object emitted by ledger stream
// Ref: https://xrpl.org/subscribe.html#ledger-stream
type LedgerStream struct {
	Type             string `json:"type,omitempty"`
	LedgerHash       string `json:"ledger_hash,omitempty"`
	ValidatedLedgers string `json:"validated_ledgers,omitempty"`
	FeeBase          uint64 `json:"fee_base,omitempty"`
	FeeRef           uint64 `json:"fee_ref,omitempty"`
	ReserveBase      uint64 `json:"reserve_base,omitempty"`
	ReserveInc       uint64 `json:"reserve_inc,omitempty"`
	LedgerIndex      uint32 `json:"ledger_index,omitempty"`
	LedgerTime       uint32 `json:"ledger_time,omitempty"`
	TxnCount         uint32 `json:"txn_count,omitempty"`
}

func (ledger *LedgerStream) Validate() error {
	if ledger.Type != LEDGER_STREAM_TYPE {
		return errors.New("invalid LedgerStream object")
	}
	if ledger.LedgerIndex < GENESIS_LEDGER {
		return errors.New("invalid ledger_index")
	}
	return nil
}

// Fetches all transaction for a specific ledger from XRPL server
func (ledger *LedgerStream) FetchTransactions() (xrpl.BaseResponse, error) {
	if err := ledger.Validate(); err != nil {
		return nil, errors.New("invalid ledger_index")
	}

	requestId := fmt.Sprintf("ledger.%v.tx", ledger.LedgerIndex)
	request := xrpl.BaseRequest{
		"id":           requestId,
		"command":      "ledger",
		"ledger_index": ledger.LedgerIndex,
		"transactions": true,
		"expand":       true,
	}

	logger.Log.Debug().
		Uint32("ledger_index", ledger.LedgerIndex).
		Str("request_id", requestId).
		Msg("Sending ledger request to XRPL server")

	// Prefer RPC client to avoid contention with streaming client
	client := connections.GetXRPLRequestClient()

	// Check connection health before making request
	if err := connections.CheckXRPLRPCConnectionHealth(); err != nil {
		logger.Log.Warn().
			Uint32("ledger_index", ledger.LedgerIndex).
			Str("request_id", requestId).
			Err(err).
			Msg("XRPL RPC connection health check failed before request")
		return nil, fmt.Errorf("connection health check failed: %w", err)
	}

	// Create context with timeout for the request
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Log request start time
	startTime := time.Now()
	logger.Log.Debug().
		Uint32("ledger_index", ledger.LedgerIndex).
		Str("request_id", requestId).
		Msg("Starting XRPL request with 30s timeout")

	// Make the request with timeout using goroutine and channel
	type result struct {
		response xrpl.BaseResponse
		err      error
	}

	resultChan := make(chan result, 1)

	go func() {
		response, err := client.Request(request)
		resultChan <- result{response: response, err: err}
	}()

	// Wait for result or timeout
	var response xrpl.BaseResponse
	var err error

	select {
	case res := <-resultChan:
		response, err = res.response, res.err
	case <-ctx.Done():
		err = fmt.Errorf("request timed out after 30 seconds: %w", ctx.Err())
	}
	if err != nil {
		requestDuration := time.Since(startTime)
		logger.Log.Error().
			Uint32("ledger_index", ledger.LedgerIndex).
			Str("request_id", requestId).
			Dur("request_duration", requestDuration).
			Err(err).
			Msg("Failed to fetch transactions from XRPL server")

		// Check if it's a timeout error
		if ctx.Err() == context.DeadlineExceeded {
			logger.Log.Warn().
				Uint32("ledger_index", ledger.LedgerIndex).
				Str("request_id", requestId).
				Dur("request_duration", requestDuration).
				Msg("XRPL request timed out after 30 seconds")
		}

		// Check if it's a WebSocket connection error
		if isWebSocketError(err) {
			logger.Log.Warn().
				Uint32("ledger_index", ledger.LedgerIndex).
				Str("request_id", requestId).
				Dur("request_duration", requestDuration).
				Err(err).
				Msg("WebSocket connection error detected")
		}

		return nil, err
	}

	requestDuration := time.Since(startTime)
	logger.Log.Debug().
		Uint32("ledger_index", ledger.LedgerIndex).
		Str("request_id", requestId).
		Dur("request_duration", requestDuration).
		Msg("Successfully received response from XRPL server")

	return response, nil
}

// isWebSocketError checks if the error is related to WebSocket connection issues
func isWebSocketError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "websocket") ||
		strings.Contains(errStr, "close 1006") ||
		strings.Contains(errStr, "unexpected EOF") ||
		strings.Contains(errStr, "connection reset")
}

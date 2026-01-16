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
// Uses retry logic with exponential backoff to handle connection failures
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

	// Prefer RPC client to avoid contention with streaming client
	client := connections.GetXRPLRequestClient()

	// Retry logic with exponential backoff
	// Increased retries and timeout to handle slow XRPL server responses
	maxRetries := 5
	baseDelay := 3 * time.Second
	maxDelay := 15 * time.Second
	requestTimeout := 60 * time.Second // Increased from 30s to handle large ledgers

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Check connection health before making request
		if err := connections.CheckXRPLRPCConnectionHealth(); err != nil {
			logger.Log.Warn().
				Uint32("ledger_index", ledger.LedgerIndex).
				Str("request_id", requestId).
				Int("attempt", attempt+1).
				Err(err).
				Msg("XRPL RPC connection health check failed before request")
			lastErr = fmt.Errorf("connection health check failed: %w", err)
			// Still retry if it's a retryable error
			if !connections.IsRetryableError(err) {
				return nil, lastErr
			}
		} else {
			// Connection is healthy, try the request
			response, err := ledger.fetchTransactionsWithTimeout(client, request, requestId, requestTimeout)
			if err == nil {
				return response, nil
			}
			lastErr = err
		}

		// Check if it's a retryable error
		if !connections.IsRetryableError(lastErr) {
			// Non-retryable error, return immediately
			return nil, fmt.Errorf("non-retryable error: %w", lastErr)
		}

		// Don't retry on last attempt
		if attempt == maxRetries-1 {
			break
		}

		// Calculate delay with exponential backoff
		delay := baseDelay * time.Duration(1<<uint(attempt))
		if delay > maxDelay {
			delay = maxDelay
		}

		// Log retry attempt
		logger.Log.Warn().
			Err(lastErr).
			Uint32("ledger_index", ledger.LedgerIndex).
			Str("request_id", requestId).
			Int("attempt", attempt+1).
			Int("max_retries", maxRetries).
			Dur("retry_in", delay).
			Msg("XRPL ledger request failed, retrying")

		// Wait before retry
		time.Sleep(delay)
	}

	// All retries exhausted
	return nil, fmt.Errorf("failed to fetch ledger after %d retries: %w", maxRetries, lastErr)
}

// fetchTransactionsWithTimeout makes a single request with timeout
func (ledger *LedgerStream) fetchTransactionsWithTimeout(client *xrpl.Client, request xrpl.BaseRequest, requestId string, timeout time.Duration) (xrpl.BaseResponse, error) {
	// Create context with timeout for the request
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Log request start time
	startTime := time.Now()
	logger.Log.Debug().
		Uint32("ledger_index", ledger.LedgerIndex).
		Str("request_id", requestId).
		Msg("Starting XRPL request with timeout")

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
		err = fmt.Errorf("request timed out after %v: %w", timeout, ctx.Err())
	}

	if err != nil {
		requestDuration := time.Since(startTime)
		logger.Log.Debug().
			Uint32("ledger_index", ledger.LedgerIndex).
			Str("request_id", requestId).
			Dur("request_duration", requestDuration).
			Err(err).
			Msg("XRPL request failed")

		// Check if it's a timeout error
		if ctx.Err() == context.DeadlineExceeded {
			logger.Log.Debug().
				Uint32("ledger_index", ledger.LedgerIndex).
				Str("request_id", requestId).
				Dur("request_duration", requestDuration).
				Msg("XRPL request timed out")
		}

		// Check if it's a WebSocket connection error
		if isWebSocketError(err) {
			logger.Log.Debug().
				Uint32("ledger_index", ledger.LedgerIndex).
				Str("request_id", requestId).
				Dur("request_duration", requestDuration).
				Err(err).
				Msg("WebSocket connection error detected")
		}

		return nil, err
	}

	// Check if response contains an error status from XRPL
	if response != nil {
		if status, ok := response["status"].(string); ok && status == "error" {
			var errorMsg string
			if errMsg, ok := response["error"].(string); ok {
				errorMsg = errMsg
			} else {
				errorMsg = "unknown error"
			}
			requestDuration := time.Since(startTime)
			logger.Log.Warn().
				Uint32("ledger_index", ledger.LedgerIndex).
				Str("request_id", requestId).
				Str("error", errorMsg).
				Dur("request_duration", requestDuration).
				Msg("XRPL API returned error status in response")
			return nil, fmt.Errorf("XRPL API error: %s", errorMsg)
		}
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

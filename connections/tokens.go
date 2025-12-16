package connections

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/xrpl-go"
)

// RateLimitError represents a rate limit error from XRPL API
type RateLimitError struct {
	ErrorCode string
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("XRPL API rate limit (slowDown): %s", e.ErrorCode)
}

// TokenInfo represents a token (currency + issuer)
type TokenInfo struct {
	Currency      string
	Issuer        string
	InLedgerIndex uint32 // Index of the transaction within the ledger where this token was found
}

// AreTokensKnownBatch checks if tokens exist in known_tokens table
// Returns a map of token key (currency|issuer) to whether it's known
func AreTokensKnownBatch(ctx context.Context, tokens []TokenInfo) (map[string]bool, error) {

	if ClickHouseConn == nil {
		return nil, fmt.Errorf("ClickHouse connection not initialized")
	}

	result := make(map[string]bool)

	// Filter out XRP and empty tokens
	validTokens := make([]TokenInfo, 0)
	for _, token := range tokens {
		if token.Currency == "XRP" || token.Currency == "" || token.Issuer == "" {
			key := token.Currency + "|" + token.Issuer
			result[key] = true // XRP is always "known"
			continue
		}
		validTokens = append(validTokens, token)
	}

	if len(validTokens) == 0 {
		logger.Log.Debug().Msg("No valid tokens to check in known_tokens")
		return result, nil
	}

	logger.Log.Info().
		Int("total_tokens", len(tokens)).
		Int("valid_tokens", len(validTokens)).
		Msg("Checking tokens in known_tokens table")

	// Log sample tokens being checked (first 5)
	sampleCount := 5
	if len(validTokens) < sampleCount {
		sampleCount = len(validTokens)
	}
	for i := 0; i < sampleCount; i++ {
		logger.Log.Debug().
			Str("currency", validTokens[i].Currency).
			Str("issuer", validTokens[i].Issuer).
			Msg("Checking token in known_tokens")
	}
	if len(validTokens) > sampleCount {
		logger.Log.Debug().
			Int("remaining", len(validTokens)-sampleCount).
			Msg("... and more tokens to check")
	}

	// Process in batches to avoid query size limits
	batchSize := 500
	foundCount := 0
	for i := 0; i < len(validTokens); i += batchSize {
		end := i + batchSize
		if end > len(validTokens) {
			end = len(validTokens)
		}

		batch := validTokens[i:end]

		// Build WHERE clause for batch check in known_tokens
		var conditions []string
		for _, token := range batch {
			currencyEscaped := strings.ReplaceAll(token.Currency, "'", "''")
			issuerEscaped := strings.ReplaceAll(token.Issuer, "'", "''")
			conditions = append(conditions,
				fmt.Sprintf("(currency = '%s' AND issuer = '%s')", currencyEscaped, issuerEscaped))
		}

		// Check in known_tokens table (batch query)
		query := fmt.Sprintf(`
			SELECT currency, issuer
			FROM xrpl.known_tokens 
			WHERE %s
		`, strings.Join(conditions, " OR "))

		logger.Log.Debug().Str("query", query).Msg("Executing query to check tokens in known_tokens")

		rows, err := ClickHouseConn.Query(ctx, query)
		if err != nil {
			logger.Log.Warn().
				Err(err).
				Int("batch_start", i+1).
				Int("batch_end", end).
				Msg("Error querying known_tokens batch")
			continue
		}

		batchFound := 0
		for rows.Next() {
			var currency, issuer string
			if err := rows.Scan(&currency, &issuer); err == nil {
				key := currency + "|" + issuer
				result[key] = true
				batchFound++
				foundCount++
			}
		}
		rows.Close()

		logger.Log.Debug().
			Int("batch_start", i+1).
			Int("batch_end", end).
			Int("batch_size", len(batch)).
			Int("found_in_batch", batchFound).
			Msg("Checked token batch in known_tokens")
	}

	// Initialize all tokens in result map (set to false if not found)
	notFoundCount := 0
	for _, token := range validTokens {
		key := token.Currency + "|" + token.Issuer
		if _, exists := result[key]; !exists {
			result[key] = false
			notFoundCount++
		}
	}

	logger.Log.Info().
		Int("total_checked", len(validTokens)).
		Int("found_in_db", foundCount).
		Int("not_found", notFoundCount).
		Msg("Finished checking tokens in known_tokens")

	return result, nil
}

// AddNewToken adds a new token to both new_tokens and known_tokens tables
func AddNewToken(ctx context.Context, currency, issuer string, ledgerIndex uint32, inLedgerIndex uint32, timestamp time.Time) error {
	if ClickHouseConn == nil {
		return fmt.Errorf("ClickHouse connection not initialized")
	}

	// Format timestamp for ClickHouse DateTime64(3, 'UTC')
	timestampStr := timestamp.UTC().Format("2006-01-02 15:04:05.000")

	// Escape single quotes in currency and issuer to prevent SQL injection
	currencyEscaped := strings.ReplaceAll(currency, "'", "''")
	issuerEscaped := strings.ReplaceAll(issuer, "'", "''")

	// Version for ReplacingMergeTree (use Unix timestamp)
	version := uint64(timestamp.UTC().Unix())

	// Insert into new_tokens table
	queryNewTokens := fmt.Sprintf(`
		INSERT INTO xrpl.new_tokens 
		(currency_code, issuer, first_seen_ledger_index, first_seen_in_ledger_index, create_timestamp)
		VALUES ('%s', '%s', %d, %d, toDateTime64('%s', 3, 'UTC'))
	`, currencyEscaped, issuerEscaped, ledgerIndex, inLedgerIndex, timestampStr)

	err := ClickHouseConn.Exec(ctx, queryNewTokens)
	if err != nil {
		return fmt.Errorf("failed to insert new token into new_tokens: %w", err)
	}

	// Insert into known_tokens table to prevent duplicates
	queryKnownTokens := fmt.Sprintf(`
		INSERT INTO xrpl.known_tokens 
		(currency, issuer, version)
		VALUES ('%s', '%s', %d)
	`, currencyEscaped, issuerEscaped, version)

	err = ClickHouseConn.Exec(ctx, queryKnownTokens)
	if err != nil {
		return fmt.Errorf("failed to insert new token into known_tokens: %w", err)
	}

	return nil
}

// CheckTokenViaXRPLAPI checks if a token has transaction history via XRPL account_tx API
// Uses retry logic with exponential backoff to handle connection failures
func CheckTokenViaXRPLAPI(ctx context.Context, currency, issuer string) (bool, error) {
	client := GetXRPLRequestClient()
	if client == nil {
		return false, fmt.Errorf("XRPL RPC client not initialized")
	}

	// Request account_tx for the issuer with limit
	// Reduced to 500 to avoid "too much load on the server" errors
	request := xrpl.BaseRequest{
		"command": "account_tx",
		"account": issuer,
		"limit":   200, // Check last 500 transactions (reduced from 1000 to avoid server load)
	}

	// Retry logic with exponential backoff
	maxRetries := 5
	baseDelay := 1 * time.Second
	maxDelay := 30 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		response, err := client.Request(request)
		if err == nil {
			// Success, process response
			hasHistory, processErr := processAccountTxResponse(response, currency)
			if processErr == nil {
				return hasHistory, nil
			}

			// Check if it's a rate limit error (slowDown)
			var rateLimitErr *RateLimitError
			if strings.Contains(processErr.Error(), "slowDown") ||
				errors.As(processErr, &rateLimitErr) {
				lastErr = processErr
				// Use longer delay for rate limiting (2-10 seconds)
				delay := 2 * time.Second * time.Duration(attempt+1)
				if delay > 10*time.Second {
					delay = 10 * time.Second
				}

				logger.Log.Warn().
					Err(processErr).
					Str("currency", currency).
					Str("issuer", issuer).
					Int("attempt", attempt+1).
					Int("max_retries", maxRetries).
					Dur("retry_in", delay).
					Msg("XRPL API rate limit (slowDown), retrying with delay")

				select {
				case <-ctx.Done():
					return false, fmt.Errorf("context cancelled during rate limit retry: %w", ctx.Err())
				case <-time.After(delay):
					// Continue to next retry
					continue
				}
			}

			// Other errors from processing
			lastErr = processErr
			if !IsRetryableError(processErr) {
				return false, fmt.Errorf("non-retryable error: %w", processErr)
			}
		} else {
			lastErr = err
		}

		// Check if it's a retryable error
		if !IsRetryableError(err) {
			// Non-retryable error, return immediately
			return false, fmt.Errorf("non-retryable error: %w", err)
		}

		// Calculate delay with exponential backoff
		delay := baseDelay * time.Duration(1<<uint(attempt))
		if delay > maxDelay {
			delay = maxDelay
		}

		// Log retry attempt
		logger.Log.Warn().
			Err(err).
			Str("currency", currency).
			Str("issuer", issuer).
			Int("attempt", attempt+1).
			Int("max_retries", maxRetries).
			Dur("retry_in", delay).
			Msg("XRPL API request failed, retrying")

		// Wait before retry
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("context cancelled during retry: %w", ctx.Err())
		case <-time.After(delay):
			// Continue to next retry
		}
	}

	// All retries exhausted
	return false, fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

// hasTokenInRippleState checks if a RippleState entry contains the specified currency
func hasTokenInRippleState(fields map[string]interface{}, currency string) bool {
	highLimit, _ := fields["HighLimit"].(map[string]interface{})
	lowLimit, _ := fields["LowLimit"].(map[string]interface{})

	if highLimit != nil {
		if curr, _ := highLimit["currency"].(string); curr == currency {
			return true
		}
	}

	if lowLimit != nil {
		if curr, _ := lowLimit["currency"].(string); curr == currency {
			return true
		}
	}

	return false
}

// processAccountTxResponse processes the account_tx API response
func processAccountTxResponse(response xrpl.BaseResponse, currency string) (bool, error) {
	// Check if response has error
	if status, ok := response["status"].(string); ok && status == "error" {
		if errorMsg, ok := response["error"].(string); ok {
			// Log full response for debugging
			logger.Log.Warn().
				Interface("full_response", response).
				Str("currency", currency).
				Str("error", errorMsg).
				Msg("XRPL API returned error status, logging full response")
			return false, fmt.Errorf("XRPL API error: %s", errorMsg)
		}
		// Log full response even if error message is missing
		logger.Log.Warn().
			Interface("full_response", response).
			Str("currency", currency).
			Msg("XRPL API returned error status without error message, logging full response")
		return false, fmt.Errorf("XRPL API error: unknown error")
	}

	// Check if response has result
	result, ok := response["result"].(map[string]interface{})
	if !ok {
		// Log the actual response for debugging
		logger.Log.Warn().
			Interface("full_response", response).
			Str("currency", currency).
			Msg("Invalid response format: no result field, logging full response")
		return false, fmt.Errorf("invalid response format: no result field")
	}

	// Check for error
	if errorCode, ok := result["error"].(string); ok && errorCode != "" {
		// If account not found or no transactions, token is new
		if errorCode == "actNotFound" {
			return false, nil
		}
		// slowDown is a rate limit error - should be retried with delay
		// Log full response for debugging
		if errorCode == "slowDown" {
			logger.Log.Warn().
				Interface("full_response", response).
				Interface("result", result).
				Str("currency", currency).
				Str("error_code", errorCode).
				Msg("XRPL API returned slowDown error, logging full response")
			return false, &RateLimitError{ErrorCode: errorCode}
		}
		// Log other errors too
		logger.Log.Warn().
			Interface("full_response", response).
			Interface("result", result).
			Str("currency", currency).
			Str("error_code", errorCode).
			Msg("XRPL API returned error, logging full response")
		return false, fmt.Errorf("XRPL API error: %s", errorCode)
	}

	// Get transactions array
	transactions, ok := result["transactions"].([]interface{})
	if !ok {
		// No transactions means new token
		return false, nil
	}

	// Check if any transaction uses this currency
	// Use the same extraction logic as ExtractTokensFromTransaction to be consistent
	for _, txObj := range transactions {
		tx, ok := txObj.(map[string]interface{})
		if !ok {
			continue
		}

		// Check tx field (transaction data)
		txData, ok := tx["tx"].(map[string]interface{})
		if !ok {
			continue
		}

		// Check Amount field
		if amount, ok := txData["Amount"].(map[string]interface{}); ok {
			if curr, _ := amount["currency"].(string); curr == currency {
				if issuerAddr, _ := amount["issuer"].(string); issuerAddr == "" || issuerAddr == currency {
					// XRP or invalid, skip
					continue
				}
				// Found transaction with this currency
				return true, nil
			}
		}

		// Check SendMax field
		if sendMax, ok := txData["SendMax"].(map[string]interface{}); ok {
			if curr, _ := sendMax["currency"].(string); curr == currency {
				if issuerAddr, _ := sendMax["issuer"].(string); issuerAddr == "" || issuerAddr == currency {
					continue
				}
				return true, nil
			}
		}

		// Check DeliverMin field
		if deliverMin, ok := txData["DeliverMin"].(map[string]interface{}); ok {
			if curr, _ := deliverMin["currency"].(string); curr == currency {
				if issuerAddr, _ := deliverMin["issuer"].(string); issuerAddr == "" || issuerAddr == currency {
					continue
				}
				return true, nil
			}
		}

		// Check LimitAmount field (TrustSet)
		if limitAmount, ok := txData["LimitAmount"].(map[string]interface{}); ok {
			if curr, _ := limitAmount["currency"].(string); curr == currency {
				if issuerAddr, _ := limitAmount["issuer"].(string); issuerAddr == "" || issuerAddr == currency {
					continue
				}
				return true, nil
			}
		}

		// Check meta.delivered_amount
		if meta, ok := tx["meta"].(map[string]interface{}); ok {
			if deliveredAmount, ok := meta["delivered_amount"].(map[string]interface{}); ok {
				if curr, _ := deliveredAmount["currency"].(string); curr == currency {
					return true, nil
				}
			}
			if deliveredAmount, ok := meta["DeliveredAmount"].(map[string]interface{}); ok {
				if curr, _ := deliveredAmount["currency"].(string); curr == currency {
					return true, nil
				}
			}

			// Check meta.AffectedNodes for RippleState entries
			if nodes, ok := meta["AffectedNodes"].([]interface{}); ok {
				for _, rawNode := range nodes {
					nodeMap, ok := rawNode.(map[string]interface{})
					if !ok {
						continue
					}

					// Check CreatedNode
					if createdNode, ok := nodeMap["CreatedNode"].(map[string]interface{}); ok {
						if ledgerType, _ := createdNode["LedgerEntryType"].(string); ledgerType == "RippleState" {
							if newFields, ok := createdNode["NewFields"].(map[string]interface{}); ok {
								if hasTokenInRippleState(newFields, currency) {
									return true, nil
								}
							}
						}
					}

					// Check ModifiedNode
					if modifiedNode, ok := nodeMap["ModifiedNode"].(map[string]interface{}); ok {
						if ledgerType, _ := modifiedNode["LedgerEntryType"].(string); ledgerType == "RippleState" {
							if finalFields, ok := modifiedNode["FinalFields"].(map[string]interface{}); ok {
								if hasTokenInRippleState(finalFields, currency) {
									return true, nil
								}
							}
						}
					}
				}
			}
		}
	}

	// No transactions found with this currency
	return false, nil
}

// IsRetryableError checks if an error is retryable (network/connection issues)
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()

	// Network/connection errors are retryable
	retryablePatterns := []string{
		"connection",
		"timeout",
		"network",
		"EOF",
		"broken pipe",
		"connection reset",
		"no such host",
		"temporary failure",
		"i/o timeout",
		"context deadline exceeded",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(strings.ToLower(errStr), strings.ToLower(pattern)) {
			return true
		}
	}

	// Check for IP limit errors (these are also retryable but with longer delay)
	// Reuse the same function from xrpl_rpc.go
	errStrLower := strings.ToLower(errStr)
	if strings.Contains(errStrLower, "close 1008") ||
		strings.Contains(errStrLower, "policy violation") ||
		strings.Contains(errStrLower, "ip limit reached") {
		return true
	}

	// Check for rate limiting errors from XRPL API
	if strings.Contains(errStrLower, "slowdown") ||
		strings.Contains(errStrLower, "rate limit") ||
		strings.Contains(errStrLower, "too many requests") {
		return true
	}

	return false
}

package connections

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/xrpl-go"
)

// TokenInfo represents a token (currency + issuer)
type TokenInfo struct {
	Currency      string
	Issuer        string
	InLedgerIndex uint32 // Index of the transaction within the ledger where this token was found
}

// IsTokenKnown checks if a token exists in the known_tokens table or in money_flow
// This is a single-token check function (kept for backward compatibility)
func IsTokenKnown(ctx context.Context, currency, issuer string) (bool, error) {
	if ClickHouseConn == nil {
		return false, fmt.Errorf("ClickHouse connection not initialized")
	}

	// Skip XRP
	if currency == "XRP" || currency == "" {
		return true, nil
	}

	// Escape single quotes for SQL safety
	currencyEscaped := strings.ReplaceAll(currency, "'", "''")
	issuerEscaped := strings.ReplaceAll(issuer, "'", "''")

	// First check in known_tokens table
	query := fmt.Sprintf(`
		SELECT 1 
		FROM xrpl.known_tokens 
		WHERE currency = '%s' AND issuer = '%s'
		LIMIT 1
	`, currencyEscaped, issuerEscaped)

	var result int
	err := ClickHouseConn.QueryRow(ctx, query).Scan(&result)
	if err == nil {
		// Found in known_tokens
		return true, nil
	}

	// If not found in known_tokens, check in money_flow
	query2 := fmt.Sprintf(`
		SELECT 1 
		FROM xrpl.money_flow 
		WHERE (from_currency = '%s' AND from_issuer_address = '%s')
		   OR (to_currency = '%s' AND to_issuer_address = '%s')
		LIMIT 1
	`, currencyEscaped, issuerEscaped, currencyEscaped, issuerEscaped)

	err = ClickHouseConn.QueryRow(ctx, query2).Scan(&result)
	if err == nil {
		// Found in money_flow - token exists
		return true, nil
	}

	// Not found anywhere
	return false, nil
}

// AreTokensKnownBatch checks multiple tokens at once in a single query
// Returns a map of token key (currency|issuer) to whether it's known
func AreTokensKnownBatch(ctx context.Context, tokens []TokenInfo) (map[string]bool, error) {
	if ClickHouseConn == nil {
		return nil, fmt.Errorf("ClickHouse connection not initialized")
	}

	result := make(map[string]bool)

	// Filter out XRP and empty tokens, build map
	validTokens := make([]TokenInfo, 0)
	tokenKeys := make(map[string]TokenInfo)
	for _, token := range tokens {
		if token.Currency == "XRP" || token.Currency == "" || token.Issuer == "" {
			key := token.Currency + "|" + token.Issuer
			result[key] = true // XRP is always "known"
			continue
		}
		key := token.Currency + "|" + token.Issuer
		tokenKeys[key] = token
		validTokens = append(validTokens, token)
	}

	if len(validTokens) == 0 {
		return result, nil
	}

	// Build WHERE clause for batch check in known_tokens
	var knownTokensConditions []string
	for _, token := range validTokens {
		currencyEscaped := strings.ReplaceAll(token.Currency, "'", "''")
		issuerEscaped := strings.ReplaceAll(token.Issuer, "'", "''")
		knownTokensConditions = append(knownTokensConditions,
			fmt.Sprintf("(currency = '%s' AND issuer = '%s')", currencyEscaped, issuerEscaped))
	}

	// Check in known_tokens table (batch query)
	query := fmt.Sprintf(`
		SELECT currency, issuer
		FROM xrpl.known_tokens 
		WHERE %s
	`, strings.Join(knownTokensConditions, " OR "))

	rows, err := ClickHouseConn.Query(ctx, query)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var currency, issuer string
			if err := rows.Scan(&currency, &issuer); err == nil {
				key := currency + "|" + issuer
				result[key] = true
			}
		}
	}

	// For tokens not found in known_tokens, check in money_flow
	// Build list of tokens that still need checking
	tokensToCheck := make([]TokenInfo, 0)
	for _, token := range validTokens {
		key := token.Currency + "|" + token.Issuer
		if !result[key] {
			tokensToCheck = append(tokensToCheck, token)
		}
	}

	if len(tokensToCheck) > 0 {
		// Build WHERE clause for batch check in money_flow
		var moneyFlowConditions []string
		for _, token := range tokensToCheck {
			currencyEscaped := strings.ReplaceAll(token.Currency, "'", "''")
			issuerEscaped := strings.ReplaceAll(token.Issuer, "'", "''")
			moneyFlowConditions = append(moneyFlowConditions,
				fmt.Sprintf("((from_currency = '%s' AND from_issuer_address = '%s') OR (to_currency = '%s' AND to_issuer_address = '%s'))",
					currencyEscaped, issuerEscaped, currencyEscaped, issuerEscaped))
		}

		// Query money_flow - need to check both from and to sides, exclude XRP
		query2 := fmt.Sprintf(`
			SELECT DISTINCT from_currency as currency, from_issuer_address as issuer
			FROM xrpl.money_flow 
			WHERE %s AND from_currency != 'XRP'
			UNION DISTINCT
			SELECT DISTINCT to_currency as currency, to_issuer_address as issuer
			FROM xrpl.money_flow 
			WHERE %s AND to_currency != 'XRP'
		`, strings.Join(moneyFlowConditions, " OR "), strings.Join(moneyFlowConditions, " OR "))

		rows2, err := ClickHouseConn.Query(ctx, query2)
		if err == nil {
			defer rows2.Close()
			for rows2.Next() {
				var currency, issuer string
				if err := rows2.Scan(&currency, &issuer); err == nil {
					key := currency + "|" + issuer
					result[key] = true
				}
			}
		}
	}

	// Initialize all tokens in result map (set to false if not found)
	for key := range tokenKeys {
		if _, exists := result[key]; !exists {
			result[key] = false
		}
	}

	return result, nil
}

// AddKnownToken adds a token to the known_tokens table
func AddKnownToken(ctx context.Context, currency, issuer string, ledgerIndex uint32, timestamp time.Time) error {
	if ClickHouseConn == nil {
		return fmt.Errorf("ClickHouse connection not initialized")
	}

	// Escape single quotes for SQL safety
	currencyEscaped := strings.ReplaceAll(currency, "'", "''")
	issuerEscaped := strings.ReplaceAll(issuer, "'", "''")

	now := time.Now().UTC()
	version := uint64(now.Unix())

	// Format timestamps for ClickHouse DateTime64(3, 'UTC')
	// ClickHouse accepts 'YYYY-MM-DD HH:MM:SS.mmm' format
	timestampStr := timestamp.Format("2006-01-02 15:04:05.000")
	nowStr := now.Format("2006-01-02 15:04:05.000")

	query := fmt.Sprintf(`
		INSERT INTO xrpl.known_tokens 
		(currency, issuer, first_seen_ledger_index, first_seen_timestamp, created_at, version)
		VALUES ('%s', '%s', %d, toDateTime64('%s', 3, 'UTC'), toDateTime64('%s', 3, 'UTC'), %d)
	`,
		currencyEscaped,
		issuerEscaped,
		ledgerIndex,
		timestampStr,
		nowStr,
		version,
	)

	err := ClickHouseConn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to insert known token: %w", err)
	}

	return nil
}

// AddNewToken adds a new token to the new_tokens table
func AddNewToken(ctx context.Context, currency, issuer string, ledgerIndex uint32, inLedgerIndex uint32) error {
	if ClickHouseConn == nil {
		return fmt.Errorf("ClickHouse connection not initialized")
	}

	query := fmt.Sprintf(`
		INSERT INTO xrpl.new_tokens 
		(currency_code, issuer, first_seen_ledger_index, first_seen_in_ledger_index)
		VALUES ('%s', '%s', %d, %d)
	`, currency, issuer, ledgerIndex, inLedgerIndex)

	err := ClickHouseConn.Exec(ctx, query)

	if err != nil {
		return fmt.Errorf("failed to insert new token: %w", err)
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
	request := xrpl.BaseRequest{
		"command": "account_tx",
		"account": issuer,
		"limit":   1000, // Check last 1000 transactions
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
			return processAccountTxResponse(response, currency)
		}

		lastErr = err

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

// processAccountTxResponse processes the account_tx API response
func processAccountTxResponse(response xrpl.BaseResponse, currency string) (bool, error) {
	// Check if response has result
	result, ok := response["result"].(map[string]interface{})
	if !ok {
		return false, fmt.Errorf("invalid response format: no result field")
	}

	// Check for error
	if errorCode, ok := result["error"].(string); ok && errorCode != "" {
		// If account not found or no transactions, token is new
		if errorCode == "actNotFound" {
			return false, nil
		}
		return false, fmt.Errorf("XRPL API error: %s", errorCode)
	}

	// Get transactions array
	transactions, ok := result["transactions"].([]interface{})
	if !ok {
		// No transactions means new token
		return false, nil
	}

	// Check if any transaction uses this currency
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
				return true, nil // Found transaction with this currency
			}
		}

		// Check SendMax field
		if sendMax, ok := txData["SendMax"].(map[string]interface{}); ok {
			if curr, _ := sendMax["currency"].(string); curr == currency {
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

	return false
}

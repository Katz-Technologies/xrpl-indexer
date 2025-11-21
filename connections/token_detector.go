package connections

import (
	"context"
	"time"

	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/socketio"
)

// deepCopyMap creates a deep copy of map[string]interface{} to avoid concurrent access issues
func deepCopyMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return nil
	}
	dst := make(map[string]interface{})
	for k, v := range src {
		switch val := v.(type) {
		case map[string]interface{}:
			dst[k] = deepCopyMap(val)
		case []interface{}:
			dst[k] = deepCopySlice(val)
		default:
			dst[k] = v
		}
	}
	return dst
}

// deepCopySlice creates a deep copy of []interface{}
func deepCopySlice(src []interface{}) []interface{} {
	if src == nil {
		return nil
	}
	dst := make([]interface{}, len(src))
	for i, v := range src {
		switch val := v.(type) {
		case map[string]interface{}:
			dst[i] = deepCopyMap(val)
		case []interface{}:
			dst[i] = deepCopySlice(val)
		default:
			dst[i] = v
		}
	}
	return dst
}

// DeepCopyTransactions creates a deep copy of a slice of transactions to avoid concurrent access issues
// This should be called before passing transactions to a goroutine
func DeepCopyTransactions(transactions []interface{}) []interface{} {
	if transactions == nil {
		return nil
	}
	copied := make([]interface{}, 0, len(transactions))
	for _, txObj := range transactions {
		tx, ok := txObj.(map[string]interface{})
		if !ok {
			// If it's not a map, just copy the reference
			copied = append(copied, txObj)
			continue
		}
		// Deep copy the transaction map
		copied = append(copied, deepCopyMap(tx))
	}
	return copied
}

// ExtractTokensFromTransaction extracts all unique tokens (currency, issuer) from a transaction
// Note: The transaction map should already be copied before calling this function if it will be used concurrently
func ExtractTokensFromTransaction(tx map[string]interface{}) []TokenInfo {
	tokens := make(map[string]TokenInfo)

	// Helper function to add token
	addToken := func(currency, issuer string) {
		if currency == "" || currency == "XRP" {
			return
		}
		if issuer == "" {
			return
		}
		key := currency + "|" + issuer
		tokens[key] = TokenInfo{Currency: currency, Issuer: issuer}
	}

	// Extract from Amount field
	if amount, ok := tx["Amount"].(map[string]interface{}); ok {
		if currency, _ := amount["currency"].(string); currency != "" {
			if issuer, _ := amount["issuer"].(string); issuer != "" {
				addToken(currency, issuer)
			}
		}
	}

	// Extract from SendMax field
	if sendMax, ok := tx["SendMax"].(map[string]interface{}); ok {
		if currency, _ := sendMax["currency"].(string); currency != "" {
			if issuer, _ := sendMax["issuer"].(string); issuer != "" {
				addToken(currency, issuer)
			}
		}
	}

	// Extract from DeliverMin field
	if deliverMin, ok := tx["DeliverMin"].(map[string]interface{}); ok {
		if currency, _ := deliverMin["currency"].(string); currency != "" {
			if issuer, _ := deliverMin["issuer"].(string); issuer != "" {
				addToken(currency, issuer)
			}
		}
	}

	// Extract from LimitAmount (TrustSet)
	if limitAmount, ok := tx["LimitAmount"].(map[string]interface{}); ok {
		if currency, _ := limitAmount["currency"].(string); currency != "" {
			if issuer, _ := limitAmount["issuer"].(string); issuer != "" {
				addToken(currency, issuer)
			}
		}
	}

	// Extract from meta.AffectedNodes (RippleState)
	if meta, ok := tx["meta"].(map[string]interface{}); ok {
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
							extractTokenFromRippleState(newFields, addToken)
						}
					}
				}

				// Check ModifiedNode
				if modifiedNode, ok := nodeMap["ModifiedNode"].(map[string]interface{}); ok {
					if ledgerType, _ := modifiedNode["LedgerEntryType"].(string); ledgerType == "RippleState" {
						if finalFields, ok := modifiedNode["FinalFields"].(map[string]interface{}); ok {
							extractTokenFromRippleState(finalFields, addToken)
						}
					}
				}
			}
		}
	}

	// Convert map to slice
	result := make([]TokenInfo, 0, len(tokens))
	for _, token := range tokens {
		result = append(result, token)
	}

	return result
}

// extractTokenFromRippleState extracts token from RippleState fields
func extractTokenFromRippleState(fields map[string]interface{}, addToken func(string, string)) {
	highLimit, _ := fields["HighLimit"].(map[string]interface{})
	lowLimit, _ := fields["LowLimit"].(map[string]interface{})

	if highLimit != nil {
		if currency, _ := highLimit["currency"].(string); currency != "" {
			if issuer, _ := highLimit["issuer"].(string); issuer != "" {
				addToken(currency, issuer)
			}
		}
	}

	if lowLimit != nil {
		if currency, _ := lowLimit["currency"].(string); currency != "" {
			if issuer, _ := lowLimit["issuer"].(string); issuer != "" {
				addToken(currency, issuer)
			}
		}
	}
}

// CheckAndNotifyNewTokens checks tokens from a ledger and notifies about new ones
func CheckAndNotifyNewTokens(ctx context.Context, transactions []interface{}, ledgerIndex uint32, ledgerTimestamp time.Time) error {
	// Collect all unique tokens from all transactions
	allTokens := make(map[string]TokenInfo)

	for _, txObj := range transactions {
		tx, ok := txObj.(map[string]interface{})
		if !ok {
			continue
		}

		// Only process successful transactions
		if meta, ok := tx["meta"].(map[string]interface{}); ok {
			if result, ok := meta["TransactionResult"].(string); ok && result != "tesSUCCESS" {
				continue
			}
		}

		tokens := ExtractTokensFromTransaction(tx)
		for _, token := range tokens {
			key := token.Currency + "|" + token.Issuer
			allTokens[key] = token
		}
	}

	// Convert map to slice for batch checking
	tokenSlice := make([]TokenInfo, 0, len(allTokens))
	for _, token := range allTokens {
		tokenSlice = append(tokenSlice, token)
	}

	// Batch check all tokens in ClickHouse (single query instead of N queries)
	knownMap, err := AreTokensKnownBatch(ctx, tokenSlice)
	if err != nil {
		logger.Log.Warn().
			Err(err).
			Msg("Error batch checking tokens in ClickHouse, falling back to individual checks")
		// Fallback to individual checks
		knownMap = make(map[string]bool)
		for _, token := range tokenSlice {
			key := token.Currency + "|" + token.Issuer
			known, err := IsTokenKnown(ctx, token.Currency, token.Issuer)
			if err == nil {
				knownMap[key] = known
			}
		}
	}

	// Check each token
	newTokens := make([]TokenInfo, 0)
	tokensToAddToKnown := make([]TokenInfo, 0)

	for _, token := range tokenSlice {
		key := token.Currency + "|" + token.Issuer
		known := knownMap[key]

		if known {
			// Token already known, skip
			continue
		}

		// Not found in ClickHouse, check via XRPL API
		// Note: XRPL API doesn't support batch requests, so we check individually
		// but we could parallelize this if needed
		hasHistory, err := CheckTokenViaXRPLAPI(ctx, token.Currency, token.Issuer)
		if err != nil {
			logger.Log.Warn().
				Err(err).
				Str("currency", token.Currency).
				Str("issuer", token.Issuer).
				Msg("Error checking token via XRPL API, assuming new token")
			// On error, assume it's a new token
			newTokens = append(newTokens, token)
			continue
		}

		if !hasHistory {
			// New token found!
			newTokens = append(newTokens, token)
		} else {
			// Token has history but not in our DB, add it to known_tokens
			tokensToAddToKnown = append(tokensToAddToKnown, token)
		}
	}

	// Batch add tokens that have history but weren't in our DB
	for _, token := range tokensToAddToKnown {
		err = AddKnownToken(ctx, token.Currency, token.Issuer, ledgerIndex, ledgerTimestamp)
		if err != nil {
			logger.Log.Warn().
				Err(err).
				Str("currency", token.Currency).
				Str("issuer", token.Issuer).
				Msg("Failed to add known token to database")
		}
	}

	// Add new tokens to known_tokens and notify
	for _, token := range newTokens {
		err := AddKnownToken(ctx, token.Currency, token.Issuer, ledgerIndex, ledgerTimestamp)
		if err != nil {
			logger.Log.Error().
				Err(err).
				Str("currency", token.Currency).
				Str("issuer", token.Issuer).
				Msg("Failed to add new token to database")
			continue
		}

		// Notify via SocketIO
		socketio.GetHub().EmitNewTokenDetected(socketio.NewTokenDetectedEvent{
			Currency:    token.Currency,
			Issuer:      token.Issuer,
			LedgerIndex: ledgerIndex,
			Timestamp:   time.Now().Unix(),
		})
	}

	return nil
}

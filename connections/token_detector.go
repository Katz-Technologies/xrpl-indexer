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

// TokenExtractionInfo contains information about where a token was extracted from
type TokenExtractionInfo struct {
	TokenInfo
	TxHash      string
	TxType      string
	SourceField string // Field from which token was extracted (Amount, SendMax, etc.)
}

// ExtractTokensFromTransaction extracts all unique tokens (currency, issuer) from a transaction
// Note: The transaction map should already be copied before calling this function if it will be used concurrently
// Returns tokens with information about where they were extracted from
func ExtractTokensFromTransaction(tx map[string]interface{}, inLedgerIndex uint32) []TokenExtractionInfo {
	tokens := make(map[string]TokenExtractionInfo)

	// Get transaction hash
	txHash := ""
	if hash, ok := tx["hash"].(string); ok {
		txHash = hash
	} else if hash, ok := tx["Hash"].(string); ok {
		txHash = hash
	}

	// Get transaction type
	txType := ""
	if txTypeVal, ok := tx["TransactionType"].(string); ok {
		txType = txTypeVal
	}

	// Helper function to add token
	addToken := func(currency, issuer string, sourceField string) {
		if currency == "" || currency == "XRP" {
			return
		}
		if issuer == "" {
			return
		}
		key := currency + "|" + issuer
		// Keep the first occurrence (earliest inLedgerIndex)
		if existing, exists := tokens[key]; !exists || inLedgerIndex < existing.InLedgerIndex {
			tokens[key] = TokenExtractionInfo{
				TokenInfo: TokenInfo{
					Currency:      currency,
					Issuer:        issuer,
					InLedgerIndex: inLedgerIndex,
				},
				TxHash:      txHash,
				TxType:      txType,
				SourceField: sourceField,
			}
		}
	}

	// Helper function to extract token from a field (same logic as detectTokenIssuer in consumers/consumers.go)
	extractFromField := func(field interface{}, sourceFieldName string) {
		if m, ok := field.(map[string]interface{}); ok {
			currency, _ := m["currency"].(string)
			issuer, _ := m["issuer"].(string)
			if currency != "" && issuer != "" {
				addToken(currency, issuer, sourceFieldName)
			}
		}
	}

	// Extract from Amount field (same priority as in detectTokenIssuer)
	if amount := tx["Amount"]; amount != nil {
		extractFromField(amount, "Amount")
	}

	// Extract from SendMax field
	if sendMax := tx["SendMax"]; sendMax != nil {
		extractFromField(sendMax, "SendMax")
	}

	// Extract from DeliverMin field
	if deliverMin := tx["DeliverMin"]; deliverMin != nil {
		extractFromField(deliverMin, "DeliverMin")
	}

	// Extract from DeliverMax field
	if deliverMax := tx["DeliverMax"]; deliverMax != nil {
		extractFromField(deliverMax, "DeliverMax")
	}

	// Extract from LimitAmount (TrustSet)
	if limitAmount := tx["LimitAmount"]; limitAmount != nil {
		extractFromField(limitAmount, "LimitAmount")
	}

	// Extract from meta.DeliveredAmount / meta.delivered_amount (same priority as in detectTokenIssuer)
	if meta, ok := tx["meta"].(map[string]interface{}); ok {
		// Check DeliveredAmount (uppercase) first
		if deliveredAmount := meta["DeliveredAmount"]; deliveredAmount != nil {
			extractFromField(deliveredAmount, "meta.DeliveredAmount")
		}
		// Check delivered_amount (lowercase)
		if deliveredAmount := meta["delivered_amount"]; deliveredAmount != nil {
			extractFromField(deliveredAmount, "meta.delivered_amount")
		}
	}

	// NOTE: We do NOT extract tokens from RippleState in AffectedNodes because:
	// - RippleState entries represent trustlines between accounts
	// - The issuer in HighLimit/LowLimit is the account that set the limit, NOT the token issuer
	// - Real token issuers are only in transaction fields: Amount, SendMax, DeliverMin, DeliverMax, LimitAmount, DeliveredAmount
	// - This matches the logic in consumers/consumers.go detectTokenIssuer function

	// Convert map to slice
	result := make([]TokenExtractionInfo, 0, len(tokens))
	for _, token := range tokens {
		result = append(result, token)
	}

	return result
}

// CheckAndNotifyNewTokens checks tokens from payout money_flow records and notifies about new ones
// moneyFlows - список всех money_flow записей, сформированных в ProcessTransaction
func CheckAndNotifyNewTokens(ctx context.Context, moneyFlows []MoneyFlowRow, ledgerIndex uint32) error {
	// Filter only payout flows
	payoutFlows := make([]MoneyFlowRow, 0)
	for _, flow := range moneyFlows {
		if flow.Kind == "payout" {
			payoutFlows = append(payoutFlows, flow)
		}
	}

	if len(payoutFlows) == 0 {
		logger.Log.Debug().
			Uint32("ledger_index", ledgerIndex).
			Msg("No payout money_flow records found")
		return nil
	}

	logger.Log.Info().
		Uint32("ledger_index", ledgerIndex).
		Int("payout_flows_count", len(payoutFlows)).
		Msg("Found payout money_flow records, extracting tokens")

	// Collect all unique tokens from payout flows
	allTokens := make(map[string]TokenInfo)
	tokenToFlow := make(map[string]MoneyFlowRow) // key: currency|issuer -> flow info

	for _, flow := range payoutFlows {
		// Extract tokens from from_currency/from_issuer
		if flow.FromCurrency != "" && flow.FromCurrency != "XRP" && flow.FromIssuerAddress != "" && flow.FromIssuerAddress != "XRP" {
			key := flow.FromCurrency + "|" + flow.FromIssuerAddress
			if existing, exists := allTokens[key]; !exists || flow.InLedgerIndex < existing.InLedgerIndex {
				allTokens[key] = TokenInfo{
					Currency:      flow.FromCurrency,
					Issuer:        flow.FromIssuerAddress,
					InLedgerIndex: flow.InLedgerIndex,
				}
				tokenToFlow[key] = flow
			}
		}

		// Extract tokens from to_currency/to_issuer
		if flow.ToCurrency != "" && flow.ToCurrency != "XRP" && flow.ToIssuerAddress != "" && flow.ToIssuerAddress != "XRP" {
			key := flow.ToCurrency + "|" + flow.ToIssuerAddress
			if existing, exists := allTokens[key]; !exists || flow.InLedgerIndex < existing.InLedgerIndex {
				allTokens[key] = TokenInfo{
					Currency:      flow.ToCurrency,
					Issuer:        flow.ToIssuerAddress,
					InLedgerIndex: flow.InLedgerIndex,
				}
				tokenToFlow[key] = flow
			}
		}
	}

	// Convert map to slice
	tokenSlice := make([]TokenInfo, 0, len(allTokens))
	for _, token := range allTokens {
		tokenSlice = append(tokenSlice, token)
	}

	if len(tokenSlice) == 0 {
		logger.Log.Debug().
			Uint32("ledger_index", ledgerIndex).
			Msg("No valid tokens extracted from payout flows")
		return nil
	}

	logger.Log.Info().
		Uint32("ledger_index", ledgerIndex).
		Int("tokens_to_check", len(tokenSlice)).
		Msg("Checking tokens in known_tokens table")

	// Check which tokens exist in known_tokens table
	knownMap, err := AreTokensKnownBatch(ctx, tokenSlice)
	if err != nil {
		logger.Log.Warn().
			Err(err).
			Msg("Error checking tokens in known_tokens, skipping token detection")
		return err
	}

	// Process only tokens that are NOT in known_tokens (new tokens)
	newTokensCount := 0
	existingTokensCount := 0
	for _, token := range tokenSlice {
		key := token.Currency + "|" + token.Issuer
		if knownMap[key] {
			// Token already exists in known_tokens, skip
			existingTokensCount++
			continue
		}

		// Token is new - add to new_tokens table
		newTokensCount++

		now := time.Now().UTC()
		err := AddNewToken(ctx, token.Currency, token.Issuer, ledgerIndex, token.InLedgerIndex, now)
		if err != nil {
			logger.Log.Error().
				Err(err).
				Str("currency", token.Currency).
				Str("issuer", token.Issuer).
				Msg("Failed to add new token to database")
			continue
		}

		// Get flow info for logging tx_hash
		flow, hasFlow := tokenToFlow[key]
		txHash := ""
		if hasFlow {
			txHash = flow.TxHash
		}

		// Log new token detection
		logEntry := logger.Log.Info().
			Str("currency", token.Currency).
			Str("issuer", token.Issuer).
			Uint32("ledger_index", ledgerIndex).
			Uint32("in_ledger_index", token.InLedgerIndex)

		if txHash != "" {
			logEntry = logEntry.Str("tx_hash", txHash)
		}

		logEntry.Msg("New token detected and added to new_tokens")

		// Emit SocketIO notification
		hub := socketio.GetHub()
		event := socketio.NewTokenDetectedEvent{
			Currency:    token.Currency,
			Issuer:      token.Issuer,
			LedgerIndex: ledgerIndex,
			Timestamp:   now.Unix(),
		}
		hub.EmitNewTokenDetected(event)
	}

	logger.Log.Info().
		Uint32("ledger_index", ledgerIndex).
		Int("total_checked", len(tokenSlice)).
		Int("new_tokens", newTokensCount).
		Int("existing_tokens", existingTokensCount).
		Msg("Finished processing tokens for ledger")

	return nil
}

package connections

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/xrpscan/platform/logger"
)

const (
	sologenicAPIBaseURL = "https://api.sologenic.org/api/v1/ohlc"
	period1m            = "1m"
)

// OHLCData represents a single OHLC candle
type OHLCData struct {
	Timestamp int64
	Open      string
	High      string
	Low       string
	Close     string
	Volume    string
}

// GetUniqueTokensFromMoneyFlow gets unique tokens from money_flow table for the last month
func GetUniqueTokensFromMoneyFlow(ctx context.Context) ([]TokenInfo, error) {
	if ClickHouseConn == nil {
		return nil, fmt.Errorf("ClickHouse connection not initialized")
	}

	// Calculate date one month ago
	oneMonthAgo := time.Now().AddDate(0, -1, 0).UTC()

	query := `
		SELECT DISTINCT currency, issuer
		FROM (
			SELECT from_currency as currency, from_issuer_address as issuer
			FROM xrpl.money_flow
			FINAL
			WHERE close_time >= ?
				AND from_currency != ''
				AND from_currency != 'XRP'
				AND from_issuer_address != ''
			
			UNION ALL
			
			SELECT to_currency as currency, to_issuer_address as issuer
			FROM xrpl.money_flow
			FINAL
			WHERE close_time >= ?
				AND to_currency != ''
				AND to_currency != 'XRP'
				AND to_issuer_address != ''
		)
		WHERE currency != '' AND issuer != ''
	`

	rows, err := ClickHouseConn.Query(ctx, query, oneMonthAgo, oneMonthAgo)
	if err != nil {
		return nil, fmt.Errorf("failed to query unique tokens: %w", err)
	}
	defer rows.Close()

	var tokens []TokenInfo
	for rows.Next() {
		var token TokenInfo
		if err := rows.Scan(&token.Currency, &token.Issuer); err != nil {
			logger.Log.Warn().Err(err).Msg("Failed to scan token row")
			continue
		}
		tokens = append(tokens, token)
	}

	logger.Log.Info().
		Int("count", len(tokens)).
		Msg("Found unique tokens from money_flow")

	return tokens, nil
}

// EncodeSymbol encodes currency and issuer into Sologenic API symbol format
// Format: XRP/CURRENCY+ISSUER
// The entire symbol will be URL encoded when used in the API URL
func EncodeSymbol(currency, issuer string) string {
	return fmt.Sprintf("XRP/%s+%s", currency, issuer)
}

// FetchOHLCData fetches OHLC data from Sologenic API
func FetchOHLCData(currency, issuer string, fromTimestamp, toTimestamp int64) ([]OHLCData, error) {
	symbol := EncodeSymbol(currency, issuer)

	apiURL := fmt.Sprintf("%s?symbol=%s&period=%s&from=%d&to=%d",
		sologenicAPIBaseURL,
		url.QueryEscape(symbol),
		period1m,
		fromTimestamp,
		toTimestamp,
	)

	logger.Log.Debug().
		Str("currency", currency).
		Str("issuer", issuer).
		Str("url", apiURL).
		Int64("from", fromTimestamp).
		Int64("to", toTimestamp).
		Msg("Fetching OHLC data from Sologenic API")

	// Use longer timeout for date range requests
	// Even with smaller chunks (5 days), some requests may take time
	client := &http.Client{
		Timeout: 180 * time.Second, // 3 minutes for safety
	}

	// Retry logic for network errors
	maxRetries := 3
	var resp *http.Response
	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		resp, err = client.Get(apiURL)
		if err == nil {
			break // Success
		}

		if attempt < maxRetries {
			waitTime := time.Duration(attempt) * 5 * time.Second
			logger.Log.Warn().
				Err(err).
				Int("attempt", attempt).
				Int("max_retries", maxRetries).
				Dur("wait_time", waitTime).
				Msg("Failed to fetch OHLC data, retrying...")
			time.Sleep(waitTime)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to fetch OHLC data after %d attempts: %w", maxRetries, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sologenic API returned status %d", resp.StatusCode)
	}

	var rawData [][]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&rawData); err != nil {
		return nil, fmt.Errorf("failed to decode OHLC response: %w", err)
	}

	var ohlcData []OHLCData
	for _, row := range rawData {
		if len(row) < 6 {
			logger.Log.Warn().
				Interface("row", row).
				Msg("Invalid OHLC row format, skipping")
			continue
		}

		// Parse timestamp (first element)
		timestamp, ok := row[0].(float64)
		if !ok {
			logger.Log.Warn().
				Interface("timestamp", row[0]).
				Msg("Invalid timestamp format, skipping")
			continue
		}

		// Parse price values (strings)
		open, _ := row[1].(string)
		high, _ := row[2].(string)
		low, _ := row[3].(string)
		close, _ := row[4].(string)
		volume, _ := row[5].(string)

		ohlcData = append(ohlcData, OHLCData{
			Timestamp: int64(timestamp),
			Open:      open,
			High:      high,
			Low:       low,
			Close:     close,
			Volume:    volume,
		})
	}

	logger.Log.Info().
		Str("currency", currency).
		Str("issuer", issuer).
		Int("candles_count", len(ohlcData)).
		Msg("Fetched OHLC data from Sologenic API")

	return ohlcData, nil
}

// SaveOHLCData saves OHLC data to ClickHouse
func SaveOHLCData(ctx context.Context, currency, issuer string, data []OHLCData) error {
	if ClickHouseConn == nil {
		return fmt.Errorf("ClickHouse connection not initialized")
	}

	if len(data) == 0 {
		return nil
	}

	// Prepare batch insert - explicitly specify columns to exclude version (which has DEFAULT)
	batch, err := ClickHouseConn.PrepareBatch(ctx, "INSERT INTO xrpl.token_ohlc (timestamp, currency, issuer, open, high, low, close, volume)")
	if err != nil {
		return fmt.Errorf("failed to prepare batch insert: %w", err)
	}

	for _, candle := range data {
		timestamp := time.Unix(candle.Timestamp, 0).UTC()

		// Normalize decimal strings for ClickHouse
		open := normalizeDecimalForClickHouse(candle.Open)
		high := normalizeDecimalForClickHouse(candle.High)
		low := normalizeDecimalForClickHouse(candle.Low)
		close := normalizeDecimalForClickHouse(candle.Close)
		volume := normalizeDecimalForClickHouse(candle.Volume)

		if err := batch.Append(
			timestamp,
			currency,
			issuer,
			open,
			high,
			low,
			close,
			volume,
		); err != nil {
			logger.Log.Warn().
				Err(err).
				Str("currency", currency).
				Str("issuer", issuer).
				Int64("timestamp", candle.Timestamp).
				Msg("Failed to append OHLC row to batch")
			continue
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send OHLC batch: %w", err)
	}

	logger.Log.Info().
		Str("currency", currency).
		Str("issuer", issuer).
		Int("rows_saved", len(data)).
		Msg("Saved OHLC data to ClickHouse")

	return nil
}

// ImportTokenPrices imports OHLC prices for a token from Sologenic API
// Handles the API quirk where it returns candles from newest to oldest
// and truncates when there's no volume
// Fetches data in chunks of 10 days to avoid timeouts
func ImportTokenPrices(ctx context.Context, token TokenInfo) error {
	// Calculate time range: last month
	now := time.Now().UTC()
	oneMonthAgo := now.AddDate(0, -1, 0)

	fromTimestamp := oneMonthAgo.Unix()
	toTimestamp := now.Unix()

	// Expected number of 1-minute candles in a month
	expectedCandles := int((toTimestamp - fromTimestamp) / 60)

	logger.Log.Info().
		Str("currency", token.Currency).
		Str("issuer", token.Issuer).
		Int64("from", fromTimestamp).
		Int64("to", toTimestamp).
		Int("expected_candles", expectedCandles).
		Msg("Starting token price import")

	// Fetch data in chunks of 5 days to avoid timeouts
	// Smaller chunks are more reliable and faster
	chunkDays := 10
	chunkDuration := time.Duration(chunkDays) * 24 * time.Hour
	allCandles := make([]OHLCData, 0)

	currentTo := toTimestamp
	chunkNum := 1

	for currentTo > fromTimestamp {
		chunkFrom := currentTo - int64(chunkDuration.Seconds())
		if chunkFrom < fromTimestamp {
			chunkFrom = fromTimestamp
		}

		logger.Log.Debug().
			Str("currency", token.Currency).
			Str("issuer", token.Issuer).
			Int("chunk", chunkNum).
			Int64("chunk_from", chunkFrom).
			Int64("chunk_to", currentTo).
			Msg("Fetching OHLC data chunk")

		chunkCandles, err := FetchOHLCData(token.Currency, token.Issuer, chunkFrom, currentTo)
		if err != nil {
			logger.Log.Warn().
				Err(err).
				Str("currency", token.Currency).
				Str("issuer", token.Issuer).
				Int("chunk", chunkNum).
				Msg("Failed to fetch OHLC data chunk, continuing with next chunk")
			// Continue with next chunk instead of failing completely
			currentTo = chunkFrom
			chunkNum++
			time.Sleep(3 * time.Second) // Delay between chunks to avoid overwhelming the API
			continue
		}

		// Merge chunk candles (they are from newest to oldest)
		// Need to merge without duplicates
		existingTimestamps := make(map[int64]bool)
		for _, candle := range allCandles {
			existingTimestamps[candle.Timestamp] = true
		}

		// Add new candles that we don't already have
		for _, candle := range chunkCandles {
			if !existingTimestamps[candle.Timestamp] {
				allCandles = append(allCandles, candle)
				existingTimestamps[candle.Timestamp] = true
			}
		}

		logger.Log.Debug().
			Str("currency", token.Currency).
			Str("issuer", token.Issuer).
			Int("chunk", chunkNum).
			Int("chunk_candles", len(chunkCandles)).
			Int("total_candles", len(allCandles)).
			Msg("Merged OHLC data chunk")

		// Move to next chunk (going backwards in time)
		currentTo = chunkFrom
		chunkNum++

		// Delay between chunks to avoid overwhelming the API
		if currentTo > fromTimestamp {
			time.Sleep(3 * time.Second)
		}
	}

	if len(allCandles) == 0 {
		return fmt.Errorf("no OHLC data fetched for token")
	}

	logger.Log.Info().
		Str("currency", token.Currency).
		Str("issuer", token.Issuer).
		Int("total_candles", len(allCandles)).
		Int("expected_candles", expectedCandles).
		Msg("Fetched all OHLC data chunks")

	// Check if we got all candles for the full 30-day period
	// API returns from newest to oldest, so we need to check the oldest timestamp
	oldestTimestamp := allCandles[len(allCandles)-1].Timestamp

	// We need the oldest candle to reach the start of the period (fromTimestamp)
	// Allow 1 hour buffer for rounding/timing issues
	isOldEnough := oldestTimestamp <= fromTimestamp+3600

	// If we don't have data covering the full period, try to fetch more going further back
	maxAdditionalIterations := 10 // Increased to allow more attempts to get full 30 days
	additionalIteration := 0

	for !isOldEnough && additionalIteration < maxAdditionalIterations {
		// Find the oldest candle with volume (non-zero)
		oldestWithVolume := oldestTimestamp
		for i := len(allCandles) - 1; i >= 0; i-- {
			vol := allCandles[i].Volume
			if vol != "0" && vol != "0.0" && vol != "" {
				oldestWithVolume = allCandles[i].Timestamp
				break
			}
		}

		// If the oldest candle with volume is not old enough, fetch more data
		if oldestWithVolume > fromTimestamp+3600 {
			logger.Log.Info().
				Str("currency", token.Currency).
				Str("issuer", token.Issuer).
				Int("received_candles", len(allCandles)).
				Int("expected_candles", expectedCandles).
				Int64("oldest_timestamp", oldestTimestamp).
				Int64("oldest_with_volume", oldestWithVolume).
				Int64("target_from_timestamp", fromTimestamp).
				Int64("missing_seconds", oldestTimestamp-fromTimestamp).
				Int("additional_iteration", additionalIteration+1).
				Msg("Missing candles at the start of period, fetching additional OHLC data (going further back)")

			// Fetch data one week before the oldest candle with volume
			weekBefore := time.Unix(oldestWithVolume, 0).AddDate(0, 0, -7).Unix()

			additionalCandles, err := FetchOHLCData(token.Currency, token.Issuer, weekBefore, oldestTimestamp)
			if err != nil {
				logger.Log.Warn().
					Err(err).
					Str("currency", token.Currency).
					Str("issuer", token.Issuer).
					Msg("Failed to fetch additional OHLC data, stopping")
				break
			}

			if len(additionalCandles) == 0 {
				logger.Log.Info().
					Str("currency", token.Currency).
					Str("issuer", token.Issuer).
					Msg("No additional candles received, stopping")
				break
			}

			// Merge additional candles without duplicates
			existingTimestamps := make(map[int64]bool)
			for _, candle := range allCandles {
				existingTimestamps[candle.Timestamp] = true
			}

			// Add new candles that we don't already have
			newCandlesCount := 0
			for _, candle := range additionalCandles {
				if !existingTimestamps[candle.Timestamp] && candle.Timestamp < oldestTimestamp {
					allCandles = append(allCandles, candle)
					existingTimestamps[candle.Timestamp] = true
					newCandlesCount++
				}
			}

			logger.Log.Info().
				Str("currency", token.Currency).
				Str("issuer", token.Issuer).
				Int("additional_candles_received", len(additionalCandles)).
				Int("new_candles_added", newCandlesCount).
				Int("total_candles", len(allCandles)).
				Msg("Merged additional OHLC data")

			// Update oldest timestamp and recheck
			if len(allCandles) > 0 {
				oldestTimestamp = allCandles[len(allCandles)-1].Timestamp
				isOldEnough = oldestTimestamp <= fromTimestamp+3600
			}

			additionalIteration++
			time.Sleep(3 * time.Second) // Delay between additional requests to avoid overwhelming the API
		} else {
			break // We've reached as far back as we can go
		}
	}

	// Reverse the array since API returns newest to oldest, but we want oldest to newest for storage
	// Actually, let's keep them as is (newest to oldest) since that's how they come from API
	// ClickHouse can handle them in any order

	// Save to ClickHouse
	if err := SaveOHLCData(ctx, token.Currency, token.Issuer, allCandles); err != nil {
		return fmt.Errorf("failed to save OHLC data: %w", err)
	}

	// Final check: verify we have data covering the full 30-day period
	finalOldestTimestamp := allCandles[len(allCandles)-1].Timestamp
	hasFullCoverage := finalOldestTimestamp <= fromTimestamp+3600

	logger.Log.Info().
		Str("currency", token.Currency).
		Str("issuer", token.Issuer).
		Int("total_candles", len(allCandles)).
		Int("expected_candles", expectedCandles).
		Int64("oldest_timestamp", finalOldestTimestamp).
		Int64("target_from_timestamp", fromTimestamp).
		Bool("has_full_coverage", hasFullCoverage).
		Msg("Completed token price import")

	if !hasFullCoverage {
		logger.Log.Warn().
			Str("currency", token.Currency).
			Str("issuer", token.Issuer).
			Int64("oldest_timestamp", finalOldestTimestamp).
			Int64("target_from_timestamp", fromTimestamp).
			Int64("missing_seconds", finalOldestTimestamp-fromTimestamp).
			Msg("Warning: Could not fetch all candles for the full 30-day period - some data may be missing")
	}

	return nil
}

// CheckTokenOHLCTableEmpty checks if token_ohlc table is empty
func CheckTokenOHLCTableEmpty(ctx context.Context) (bool, error) {
	if ClickHouseConn == nil {
		return false, fmt.Errorf("ClickHouse connection not initialized")
	}

	query := "SELECT COUNT(*) FROM xrpl.token_ohlc FINAL"

	var count uint64
	if err := ClickHouseConn.QueryRow(ctx, query).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to check token_ohlc table: %w", err)
	}

	return count == 0, nil
}

// CheckTokenDataCompleteness checks if token has complete data for the last month
// Returns true if data is complete, false if incomplete or missing
func CheckTokenDataCompleteness(ctx context.Context, currency, issuer string) (bool, error) {
	if ClickHouseConn == nil {
		return false, fmt.Errorf("ClickHouse connection not initialized")
	}

	// Calculate expected time range: last month
	now := time.Now().UTC()
	oneMonthAgo := now.AddDate(0, -1, 0)
	fromTimestamp := oneMonthAgo.Unix()

	query := `
		SELECT 
			COUNT(*) AS actual_candles,
			MIN(timestamp) AS first_candle,
			MAX(timestamp) AS last_candle
		FROM xrpl.token_ohlc
		FINAL
		WHERE currency = ?
		  AND issuer = ?
	`

	var actualCandles uint64
	var firstCandle, lastCandle time.Time

	if err := ClickHouseConn.QueryRow(ctx, query, currency, issuer).Scan(&actualCandles, &firstCandle, &lastCandle); err != nil {
		// If no data exists, return false (incomplete)
		return false, nil
	}

	// Check if we have data covering the full month
	// Allow 1 hour buffer for timing issues
	firstCandleUnix := firstCandle.Unix()
	if firstCandleUnix > fromTimestamp+3600 {
		return false, nil // Data doesn't start early enough
	}

	// Check if we have enough candles (at least 95% of expected)
	expectedCandles := int((now.Unix() - fromTimestamp) / 60)
	if actualCandles < uint64(expectedCandles*95/100) {
		return false, nil // Not enough candles
	}

	return true, nil
}

// GetTokensNeedingImport gets tokens from money_flow that need import or re-import
func GetTokensNeedingImport(ctx context.Context) ([]TokenInfo, error) {
	if ClickHouseConn == nil {
		return nil, fmt.Errorf("ClickHouse connection not initialized")
	}

	// Get all unique tokens from money_flow for the last month
	allTokens, err := GetUniqueTokensFromMoneyFlow(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get unique tokens: %w", err)
	}

	if len(allTokens) == 0 {
		return nil, nil
	}

	// Check which tokens need import
	tokensNeedingImport := make([]TokenInfo, 0)

	for _, token := range allTokens {
		isComplete, err := CheckTokenDataCompleteness(ctx, token.Currency, token.Issuer)
		if err != nil {
			logger.Log.Warn().
				Err(err).
				Str("currency", token.Currency).
				Str("issuer", token.Issuer).
				Msg("Failed to check token data completeness, will try to import")
			// If check failed, assume we need to import
			tokensNeedingImport = append(tokensNeedingImport, token)
			continue
		}

		if !isComplete {
			tokensNeedingImport = append(tokensNeedingImport, token)
		}
	}

	logger.Log.Info().
		Int("total_tokens", len(allTokens)).
		Int("tokens_needing_import", len(tokensNeedingImport)).
		Msg("Checked tokens for completeness")

	return tokensNeedingImport, nil
}

// ImportAllTokenPrices imports prices for all unique tokens from money_flow
// If table is empty, imports all tokens. If table is not empty, checks completeness
// and imports only tokens that are missing or incomplete
func ImportAllTokenPrices(ctx context.Context) error {
	logger.Log.Info().Msg("Starting import of all token prices from Sologenic API")

	// Check if table is empty
	isEmpty, err := CheckTokenOHLCTableEmpty(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if token_ohlc table is empty: %w", err)
	}

	var tokens []TokenInfo

	if isEmpty {
		logger.Log.Info().Msg("token_ohlc table is empty, will import all tokens from money_flow")
		// Get all unique tokens
		tokens, err = GetUniqueTokensFromMoneyFlow(ctx)
		if err != nil {
			return fmt.Errorf("failed to get unique tokens: %w", err)
		}
	} else {
		logger.Log.Info().Msg("token_ohlc table is not empty, checking for incomplete tokens")
		// Get tokens that need import or re-import
		tokens, err = GetTokensNeedingImport(ctx)
		if err != nil {
			return fmt.Errorf("failed to get tokens needing import: %w", err)
		}

		if len(tokens) == 0 {
			logger.Log.Info().Msg("All tokens have complete data, no import needed")
			return nil
		}

		logger.Log.Info().
			Int("tokens_to_import", len(tokens)).
			Msg("Found tokens that need import or re-import")
	}

	if len(tokens) == 0 {
		logger.Log.Info().Msg("No tokens found in money_flow, skipping import")
		return nil
	}

	logger.Log.Info().
		Int("tokens_count", len(tokens)).
		Msg("Found tokens to import prices for")

	// Import prices for each token
	// Process sequentially to avoid overwhelming the API and rate limiting
	// batchSize is used only for logging and adding pauses between groups
	batchSize := 5
	for i := 0; i < len(tokens); i += batchSize {
		end := i + batchSize
		if end > len(tokens) {
			end = len(tokens)
		}

		batch := tokens[i:end]
		logger.Log.Info().
			Int("batch_start", i+1).
			Int("batch_end", end).
			Int("batch_size", len(batch)).
			Msg("Processing token batch (sequential)")

		// Process tokens sequentially (one after another)
		for _, token := range batch {
			if err := ImportTokenPrices(ctx, token); err != nil {
				logger.Log.Error().
					Err(err).
					Str("currency", token.Currency).
					Str("issuer", token.Issuer).
					Msg("Failed to import token prices, continuing with next token")
				// Continue with next token instead of failing completely
				continue
			}

			// Add delay between requests to avoid rate limiting
			time.Sleep(3 * time.Second)
		}

		// Add delay between batches
		if end < len(tokens) {
			time.Sleep(3 * time.Second)
		}
	}

	logger.Log.Info().
		Int("total_tokens", len(tokens)).
		Msg("Completed import of all token prices")

	return nil
}

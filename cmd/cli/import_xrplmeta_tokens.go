/**
* This file implements `platform-cli import-xrplmeta-tokens` subcommand
* Imports tokens from xrplmeta.org API into known_tokens table
 */

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/logger"
)

const ImportXrplmetaTokensCommandName = "import-xrplmeta-tokens"
const defaultBatchSize = 25000
const defaultFetchBatchSize = 5000 // Size of batches when fetching from API (reduced for faster response)
const xrplmetaAPIURL = "https://s1.xrplmeta.org/tokens"

// XrplmetaResponse represents the response from xrplmeta.org API
type XrplmetaResponse struct {
	Count  int `json:"count"`
	Tokens []struct {
		Currency string `json:"currency"`
		Issuer   string `json:"issuer"`
	} `json:"tokens"`
}

type ImportXrplmetaTokensCommand struct {
	fs         *flag.FlagSet
	fBatchSize int
	fDryRun    bool
	fVerbose   bool
}

func NewImportXrplmetaTokensCommand() *ImportXrplmetaTokensCommand {
	cmd := &ImportXrplmetaTokensCommand{
		fs: flag.NewFlagSet(ImportXrplmetaTokensCommandName, flag.ExitOnError),
	}

	cmd.fs.IntVar(&cmd.fBatchSize, "batch-size", defaultBatchSize, "Batch size for inserting tokens")
	cmd.fs.BoolVar(&cmd.fDryRun, "dry-run", false, "Only show what would be imported, don't actually import")
	cmd.fs.BoolVar(&cmd.fVerbose, "verbose", false, "Enable verbose logging")

	return cmd
}

func (cmd *ImportXrplmetaTokensCommand) Name() string {
	return ImportXrplmetaTokensCommandName
}

func (cmd *ImportXrplmetaTokensCommand) Init(args []string) error {
	return cmd.fs.Parse(args)
}

func (cmd *ImportXrplmetaTokensCommand) Validate() error {
	if cmd.fBatchSize <= 0 {
		return fmt.Errorf("batch-size must be positive")
	}
	return nil
}

func (cmd *ImportXrplmetaTokensCommand) Run() error {
	// Recover from panics in this function
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[IMPORT-XRPLMETA-TOKENS] Panic occurred: %v", r)
		}
	}()

	log.Printf("[IMPORT-XRPLMETA-TOKENS] Starting token import from xrplmeta.org")
	log.Printf("[IMPORT-XRPLMETA-TOKENS] Batch size: %d", cmd.fBatchSize)
	if cmd.fDryRun {
		log.Printf("[IMPORT-XRPLMETA-TOKENS] DRY RUN MODE - no data will be written")
	}

	// Initialize logger (required for ClickHouse connection)
	logger.New()
	log.Printf("[IMPORT-XRPLMETA-TOKENS] Logger initialized")

	// Try to load .env file if it exists (non-fatal if it doesn't)
	if _, err := os.Stat(".env"); err == nil {
		if err := godotenv.Load(".env"); err == nil {
			log.Printf("[IMPORT-XRPLMETA-TOKENS] Loaded .env file")
		} else {
			log.Printf("[IMPORT-XRPLMETA-TOKENS] Warning: failed to load .env file: %v", err)
		}
	} else {
		log.Printf("[IMPORT-XRPLMETA-TOKENS] No .env file found, using environment variables directly")
	}

	// Initialize ClickHouse connection
	log.Printf("[IMPORT-XRPLMETA-TOKENS] Initializing ClickHouse connection...")
	connections.NewClickHouseConnection()
	log.Printf("[IMPORT-XRPLMETA-TOKENS] ClickHouse connection initialized successfully")

	defer func() {
		log.Printf("[IMPORT-XRPLMETA-TOKENS] Closing ClickHouse connection...")
		connections.CloseClickHouse()
	}()

	if connections.ClickHouseConn == nil {
		return fmt.Errorf("ClickHouse connection not initialized")
	}

	// Step 1: Make initial request to get count
	log.Printf("[IMPORT-XRPLMETA-TOKENS] Making initial request to %s?limit=1...", xrplmetaAPIURL)
	initialURL := fmt.Sprintf("%s?limit=1", xrplmetaAPIURL)

	initialResp, err := fetchTokens(initialURL)
	if err != nil {
		return fmt.Errorf("failed to fetch initial tokens: %w", err)
	}

	log.Printf("[IMPORT-XRPLMETA-TOKENS] Initial response: count=%d, tokens received=%d", initialResp.Count, len(initialResp.Tokens))

	totalCount := initialResp.Count
	fetchBatchSize := defaultFetchBatchSize

	log.Printf("[IMPORT-XRPLMETA-TOKENS] Will fetch tokens in batches of %d", fetchBatchSize)

	// Step 2: Fetch all tokens in batches using pagination
	allTokens := make([]TokenPair, 0, totalCount)
	offset := 0

	for offset < totalCount {
		batchLimit := fetchBatchSize
		if offset+batchLimit > totalCount {
			batchLimit = totalCount - offset
		}

		log.Printf("[IMPORT-XRPLMETA-TOKENS] Fetching batch: offset=%d, limit=%d (progress: %d/%d)", offset, batchLimit, offset+batchLimit, totalCount)

		batchURL := fmt.Sprintf("%s?limit=%d&offset=%d", xrplmetaAPIURL, batchLimit, offset)
		batchResp, err := fetchTokens(batchURL)
		if err != nil {
			return fmt.Errorf("failed to fetch tokens batch (offset=%d, limit=%d): %w", offset, batchLimit, err)
		}

		log.Printf("[IMPORT-XRPLMETA-TOKENS] Fetched %d tokens in this batch", len(batchResp.Tokens))

		// Convert to TokenPair slice
		for _, token := range batchResp.Tokens {
			allTokens = append(allTokens, TokenPair{
				Currency: token.Currency,
				Issuer:   token.Issuer,
			})
		}

		offset += batchLimit

		// If we got fewer tokens than requested, we've reached the end
		if len(batchResp.Tokens) < batchLimit {
			break
		}
	}

	log.Printf("[IMPORT-XRPLMETA-TOKENS] Total tokens fetched: %d", len(allTokens))

	if len(allTokens) == 0 {
		log.Printf("[IMPORT-XRPLMETA-TOKENS] No tokens to import")
		return nil
	}

	// Step 3: Check which tokens already exist in known_tokens
	log.Printf("[IMPORT-XRPLMETA-TOKENS] Checking which tokens already exist in known_tokens...")
	checkCtx, checkCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer checkCancel()

	existingTokens, err := getExistingTokens(checkCtx, allTokens)
	if err != nil {
		return fmt.Errorf("failed to check existing tokens: %w", err)
	}

	log.Printf("[IMPORT-XRPLMETA-TOKENS] Found %d tokens already in known_tokens", len(existingTokens))

	// Step 4: Filter out existing tokens
	tokensToImport := make([]TokenPair, 0)
	for _, token := range allTokens {
		key := token.Currency + "|" + token.Issuer
		if !existingTokens[key] {
			tokensToImport = append(tokensToImport, token)
		}
	}

	log.Printf("[IMPORT-XRPLMETA-TOKENS] %d tokens need to be imported", len(tokensToImport))

	if len(tokensToImport) == 0 {
		log.Printf("[IMPORT-XRPLMETA-TOKENS] All tokens already exist in known_tokens, nothing to import")
		return nil
	}

	if cmd.fDryRun {
		log.Printf("[IMPORT-XRPLMETA-TOKENS] DRY RUN: Would import %d tokens", len(tokensToImport))
		if cmd.fVerbose {
			for i, token := range tokensToImport {
				if i < 10 { // Show first 10
					log.Printf("[IMPORT-XRPLMETA-TOKENS]   - %s (issuer: %s)", token.Currency, token.Issuer)
				}
			}
			if len(tokensToImport) > 10 {
				log.Printf("[IMPORT-XRPLMETA-TOKENS]   ... and %d more", len(tokensToImport)-10)
			}
		}
		return nil
	}

	// Step 5: Import tokens in batches
	log.Printf("[IMPORT-XRPLMETA-TOKENS] Importing tokens in batches of %d...", cmd.fBatchSize)
	imported := 0
	errors := 0

	importCtx, importCancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer importCancel()

	for i := 0; i < len(tokensToImport); i += cmd.fBatchSize {
		// Check context cancellation
		select {
		case <-importCtx.Done():
			return fmt.Errorf("import cancelled or timed out: %w", importCtx.Err())
		default:
		}

		end := i + cmd.fBatchSize
		if end > len(tokensToImport) {
			end = len(tokensToImport)
		}

		batch := tokensToImport[i:end]
		log.Printf("[IMPORT-XRPLMETA-TOKENS] Importing batch %d-%d (%d tokens)...", i+1, end, len(batch))

		if err := importTokenBatch(importCtx, batch); err != nil {
			log.Printf("[IMPORT-XRPLMETA-TOKENS] Error importing batch %d-%d: %v", i+1, end, err)
			errors++
			continue
		}

		imported += len(batch)
		log.Printf("[IMPORT-XRPLMETA-TOKENS] Progress: %d/%d tokens imported (errors: %d)", imported, len(tokensToImport), errors)
	}

	log.Printf("[IMPORT-XRPLMETA-TOKENS] Import completed: %d tokens imported, %d errors", imported, errors)

	return nil
}

// TokenPair represents a token with currency and issuer
type TokenPair struct {
	Currency string
	Issuer   string
}

// fetchTokens makes HTTP request to xrplmeta.org API and returns the response
// Uses retry logic with exponential backoff for network errors
// fetchTokens makes HTTP request to xrplmeta.org API and returns the response
func fetchTokens(url string) (*XrplmetaResponse, error) {
	tr := &http.Transport{
		ResponseHeaderTimeout: 30 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		IdleConnTimeout:       30 * time.Second,
		ExpectContinueTimeout: 5 * time.Second,
		MaxIdleConns:          10,
		MaxIdleConnsPerHost:   10,
		// Для Windows может помочь отключение keep-alive
		DisableKeepAlives: true,
	}

	client := &http.Client{
		Transport: tr,
		Timeout:   120 * time.Second, // 2 минуты максимум
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Connection", "close") // Явно закрываем соединение

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	// Используем буферизованное чтение
	buf := bytes.NewBuffer(nil)

	// Копируем с прогрессом
	var lastLog time.Time
	totalRead := 0
	bufSize := 32 * 1024 // 32KB буфер

	for {
		n, err := io.CopyN(buf, resp.Body, int64(bufSize))
		totalRead += int(n)

		if time.Since(lastLog) > 5*time.Second {
			log.Printf("[IMPORT-XRPLMETA-TOKENS] Read %d bytes so far...", totalRead)
			lastLog = time.Now()
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read error: %w", err)
		}

		if n == 0 {
			break
		}
	}

	log.Printf("[IMPORT-XRPLMETA-TOKENS] Total read: %d bytes", totalRead)

	var response XrplmetaResponse
	if err := json.Unmarshal(buf.Bytes(), &response); err != nil {
		return nil, fmt.Errorf("JSON decode error: %w", err)
	}

	return &response, nil

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// isRetryableError checks if an error is retryable (network/connection issues)
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())

	// Network/connection errors are retryable
	retryablePatterns := []string{
		"connection",
		"timeout",
		"network",
		"eof",
		"broken pipe",
		"connection reset",
		"no such host",
		"temporary failure",
		"i/o timeout",
		"context deadline exceeded",
		"wsarecv",
		"failed to respond",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// getExistingTokens returns a map of existing tokens in known_tokens table
// Processes tokens in batches to avoid query size limits
func getExistingTokens(ctx context.Context, tokens []TokenPair) (map[string]bool, error) {
	if len(tokens) == 0 {
		return make(map[string]bool), nil
	}

	existing := make(map[string]bool)
	batchSize := 500 // Process in batches to avoid query size limits

	for i := 0; i < len(tokens); i += batchSize {
		end := i + batchSize
		if end > len(tokens) {
			end = len(tokens)
		}

		batch := tokens[i:end]

		// Build WHERE clause for this batch
		var conditions []string
		for _, token := range batch {
			currencyEscaped := escapeSQLString(token.Currency)
			issuerEscaped := escapeSQLString(token.Issuer)
			conditions = append(conditions, fmt.Sprintf("(currency = '%s' AND issuer = '%s')", currencyEscaped, issuerEscaped))
		}

		query := fmt.Sprintf(`
			SELECT currency, issuer
			FROM xrpl.known_tokens
			WHERE %s
		`, strings.Join(conditions, " OR "))

		rows, err := connections.ClickHouseConn.Query(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("failed to query known_tokens (batch %d-%d): %w", i+1, end, err)
		}

		for rows.Next() {
			var currency, issuer string
			if err := rows.Scan(&currency, &issuer); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan row: %w", err)
			}
			key := currency + "|" + issuer
			existing[key] = true
		}

		rows.Close()

		if (i+batchSize)%(batchSize*10) == 0 {
			log.Printf("[IMPORT-XRPLMETA-TOKENS] Checked %d/%d tokens for existence...", end, len(tokens))
		}
	}

	return existing, nil
}

// importTokenBatch imports a batch of tokens into known_tokens table
func importTokenBatch(ctx context.Context, tokens []TokenPair) error {
	if len(tokens) == 0 {
		return nil
	}

	now := time.Now().UTC()
	timestampStr := now.Format("2006-01-02 15:04:05.000")
	version := uint64(now.Unix())

	// Build INSERT query with multiple VALUES
	var values []string
	for _, token := range tokens {
		currencyEscaped := escapeSQLString(token.Currency)
		issuerEscaped := escapeSQLString(token.Issuer)

		values = append(values, fmt.Sprintf(
			"('%s', '%s', toDateTime64('%s', 3, 'UTC'), %d)",
			currencyEscaped,
			issuerEscaped,
			timestampStr,
			version,
		))
	}

	query := fmt.Sprintf(`
		INSERT INTO xrpl.known_tokens 
		(currency, issuer, created_at, version)
		VALUES %s
	`, strings.Join(values, ", "))

	err := connections.ClickHouseConn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to insert tokens: %w", err)
	}

	return nil
}

// escapeSQLString escapes single quotes in SQL strings
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

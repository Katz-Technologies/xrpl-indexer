/**
* This file implements `platform-cli import-tokens` subcommand
* Imports existing tokens from money_flow table into known_tokens table
 */

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/joho/godotenv"
	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/logger"
)

const ImportTokensCommandName = "import-tokens"
const defaultBatchSize = 1000

type ImportTokensCommand struct {
	fs         *flag.FlagSet
	fBatchSize int
	fDryRun    bool
	fVerbose   bool
}

func NewImportTokensCommand() *ImportTokensCommand {
	cmd := &ImportTokensCommand{
		fs: flag.NewFlagSet(ImportTokensCommandName, flag.ExitOnError),
	}

	cmd.fs.IntVar(&cmd.fBatchSize, "batch-size", defaultBatchSize, "Batch size for inserting tokens")
	cmd.fs.BoolVar(&cmd.fDryRun, "dry-run", false, "Only show what would be imported, don't actually import")
	cmd.fs.BoolVar(&cmd.fVerbose, "verbose", false, "Enable verbose logging")

	return cmd
}

func (cmd *ImportTokensCommand) Name() string {
	return ImportTokensCommandName
}

func (cmd *ImportTokensCommand) Init(args []string) error {
	return cmd.fs.Parse(args)
}

func (cmd *ImportTokensCommand) Validate() error {
	if cmd.fBatchSize <= 0 {
		return fmt.Errorf("batch-size must be positive")
	}
	return nil
}

func (cmd *ImportTokensCommand) Run() error {
	// Recover from panics in this function
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[IMPORT-TOKENS] Panic occurred: %v", r)
		}
	}()

	log.Printf("[IMPORT-TOKENS] Starting token import from money_flow to known_tokens")
	log.Printf("[IMPORT-TOKENS] Batch size: %d", cmd.fBatchSize)
	if cmd.fDryRun {
		log.Printf("[IMPORT-TOKENS] DRY RUN MODE - no data will be written")
	}

	// Initialize logger (required for ClickHouse connection)
	logger.New()
	log.Printf("[IMPORT-TOKENS] Logger initialized")

	// Try to load .env file if it exists (non-fatal if it doesn't)
	// Variables can also be set directly in environment
	// Use godotenv directly to avoid Fatal() calls
	if _, err := os.Stat(".env"); err == nil {
		if err := godotenv.Load(".env"); err == nil {
			log.Printf("[IMPORT-TOKENS] Loaded .env file")
		} else {
			log.Printf("[IMPORT-TOKENS] Warning: failed to load .env file: %v", err)
		}
	} else {
		log.Printf("[IMPORT-TOKENS] No .env file found, using environment variables directly")
	}

	// Get ClickHouse config using config package functions
	host := config.EnvClickHouseHost()
	port := config.EnvClickHousePort()
	database := config.EnvClickHouseDatabase()
	user := config.EnvClickHouseUser()
	password := config.EnvClickHousePassword()

	log.Printf("[IMPORT-TOKENS] ClickHouse config: host=%q, port=%d, database=%q, user=%q, password_set=%v",
		host, port, database, user, password != "")

	// Initialize ClickHouse connection directly (avoiding Fatal() calls)
	log.Printf("[IMPORT-TOKENS] Initializing ClickHouse connection...")

	// Connect directly to avoid Fatal() calls
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Printf("[IMPORT-TOKENS] Connecting to ClickHouse at %s:%d...", host, port)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})

	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// Test connection
	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	// Set the connection in the connections package
	connections.ClickHouseConn = conn
	log.Printf("[IMPORT-TOKENS] ClickHouse connection initialized successfully")

	defer func() {
		log.Printf("[IMPORT-TOKENS] Closing ClickHouse connection...")
		connections.CloseClickHouse()
	}()

	if connections.ClickHouseConn == nil {
		return fmt.Errorf("ClickHouse connection not initialized")
	}

	// Step 1: Extract all unique tokens from money_flow
	log.Printf("[IMPORT-TOKENS] Extracting unique tokens from money_flow table...")
	extractCtx, extractCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer extractCancel()

	tokens, err := extractTokensFromMoneyFlow(extractCtx)
	if err != nil {
		return fmt.Errorf("failed to extract tokens: %w", err)
	}

	log.Printf("[IMPORT-TOKENS] Found %d unique tokens in money_flow", len(tokens))

	if len(tokens) == 0 {
		log.Printf("[IMPORT-TOKENS] No tokens to import")
		return nil
	}

	// Step 2: Check which tokens already exist in known_tokens
	log.Printf("[IMPORT-TOKENS] Checking which tokens already exist in known_tokens...")
	checkCtx, checkCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer checkCancel()

	existingTokens, err := getExistingTokens(checkCtx, tokens)
	if err != nil {
		return fmt.Errorf("failed to check existing tokens: %w", err)
	}

	log.Printf("[IMPORT-TOKENS] Found %d tokens already in known_tokens", len(existingTokens))

	// Step 3: Filter out existing tokens
	tokensToImport := make([]TokenWithMetadata, 0)
	for _, token := range tokens {
		key := token.Currency + "|" + token.Issuer
		if !existingTokens[key] {
			tokensToImport = append(tokensToImport, token)
		}
	}

	log.Printf("[IMPORT-TOKENS] %d tokens need to be imported", len(tokensToImport))

	if len(tokensToImport) == 0 {
		log.Printf("[IMPORT-TOKENS] All tokens already exist in known_tokens, nothing to import")
		return nil
	}

	if cmd.fDryRun {
		log.Printf("[IMPORT-TOKENS] DRY RUN: Would import %d tokens", len(tokensToImport))
		if cmd.fVerbose {
			for i, token := range tokensToImport {
				if i < 10 { // Show first 10
					log.Printf("[IMPORT-TOKENS]   - %s (issuer: %s, ledger: %d)", token.Currency, token.Issuer, token.FirstLedgerIndex)
				}
			}
			if len(tokensToImport) > 10 {
				log.Printf("[IMPORT-TOKENS]   ... and %d more", len(tokensToImport)-10)
			}
		}
		return nil
	}

	// Step 4: Import tokens in batches
	log.Printf("[IMPORT-TOKENS] Importing tokens in batches of %d...", cmd.fBatchSize)
	imported := 0
	errors := 0

	importCtx, importCancel := context.WithTimeout(ctx, 10*time.Minute)
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
		log.Printf("[IMPORT-TOKENS] Importing batch %d-%d (%d tokens)...", i+1, end, len(batch))

		if err := importTokenBatch(importCtx, batch); err != nil {
			log.Printf("[IMPORT-TOKENS] Error importing batch %d-%d: %v", i+1, end, err)
			errors++
			continue
		}

		imported += len(batch)
		log.Printf("[IMPORT-TOKENS] Progress: %d/%d tokens imported (errors: %d)", imported, len(tokensToImport), errors)
	}

	log.Printf("[IMPORT-TOKENS] Import completed: %d tokens imported, %d errors", imported, errors)

	return nil
}

// TokenWithMetadata represents a token with its first occurrence metadata
type TokenWithMetadata struct {
	Currency         string
	Issuer           string
	FirstLedgerIndex uint32
	FirstTimestamp   time.Time
}

// extractTokensFromMoneyFlow extracts all unique tokens from money_flow table
// Excludes XRP tokens (currency = "XRP" and issuer = "XRP")
// Finds the first occurrence using ledger_index and in_ledger_index (since close_time is the same within a ledger)
func extractTokensFromMoneyFlow(ctx context.Context) ([]TokenWithMetadata, error) {
	log.Printf("[IMPORT-TOKENS] Building query to extract tokens from money_flow...")
	query := `
		SELECT 
			currency,
			issuer,
			first_ledger_index,
			first_timestamp
		FROM (
			SELECT 
				currency,
				issuer,
				ledger_index as first_ledger_index,
				close_time as first_timestamp,
				ROW_NUMBER() OVER (
					PARTITION BY currency, issuer 
					ORDER BY ledger_index ASC, in_ledger_index ASC
				) as rn
			FROM (
				SELECT DISTINCT
					from_currency as currency,
					from_issuer_address as issuer,
					ledger_index,
					in_ledger_index,
					close_time
				FROM xrpl.money_flow
				WHERE from_currency != 'XRP' AND from_issuer_address != 'XRP'
				
				UNION DISTINCT
				
				SELECT DISTINCT
					to_currency as currency,
					to_issuer_address as issuer,
					ledger_index,
					in_ledger_index,
					close_time
				FROM xrpl.money_flow
				WHERE to_currency != 'XRP' AND to_issuer_address != 'XRP'
			) AS all_tokens
		) AS ranked_tokens
		WHERE rn = 1
		ORDER BY first_ledger_index, currency, issuer
	`

	log.Printf("[IMPORT-TOKENS] Executing query to extract tokens...")
	rows, err := connections.ClickHouseConn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query money_flow: %w", err)
	}
	defer rows.Close()

	log.Printf("[IMPORT-TOKENS] Processing query results...")
	tokens := make([]TokenWithMetadata, 0)
	rowCount := 0
	for rows.Next() {
		var token TokenWithMetadata
		var timestamp time.Time

		if err := rows.Scan(&token.Currency, &token.Issuer, &token.FirstLedgerIndex, &timestamp); err != nil {
			return nil, fmt.Errorf("failed to scan row %d: %w", rowCount, err)
		}

		token.FirstTimestamp = timestamp
		tokens = append(tokens, token)
		rowCount++

		if rowCount%10000 == 0 {
			log.Printf("[IMPORT-TOKENS] Processed %d token records...", rowCount)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	log.Printf("[IMPORT-TOKENS] Extracted %d unique tokens from money_flow", len(tokens))
	return tokens, nil
}

// getExistingTokens returns a map of existing tokens in known_tokens table
// Processes tokens in batches to avoid query size limits
func getExistingTokens(ctx context.Context, tokens []TokenWithMetadata) (map[string]bool, error) {
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
		`, joinSQLConditions(conditions, " OR "))

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
			log.Printf("[IMPORT-TOKENS] Checked %d/%d tokens for existence...", end, len(tokens))
		}
	}

	return existing, nil
}

// importTokenBatch imports a batch of tokens into known_tokens table
func importTokenBatch(ctx context.Context, tokens []TokenWithMetadata) error {
	if len(tokens) == 0 {
		return nil
	}

	// Build INSERT query with multiple VALUES
	var values []string
	for _, token := range tokens {
		currencyEscaped := escapeSQLString(token.Currency)
		issuerEscaped := escapeSQLString(token.Issuer)
		timestampStr := token.FirstTimestamp.Format("2006-01-02 15:04:05.000")
		nowStr := time.Now().UTC().Format("2006-01-02 15:04:05.000")
		version := uint64(time.Now().Unix())

		values = append(values, fmt.Sprintf(
			"('%s', '%s', %d, toDateTime64('%s', 3, 'UTC'), toDateTime64('%s', 3, 'UTC'), %d)",
			currencyEscaped,
			issuerEscaped,
			token.FirstLedgerIndex,
			timestampStr,
			nowStr,
			version,
		))
	}

	query := fmt.Sprintf(`
		INSERT INTO xrpl.known_tokens 
		(currency, issuer, first_seen_ledger_index, first_seen_timestamp, created_at, version)
		VALUES %s
	`, joinSQLConditions(values, ", "))

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

// joinSQLConditions joins SQL conditions with a separator
func joinSQLConditions(conditions []string, sep string) string {
	return strings.Join(conditions, sep)
}

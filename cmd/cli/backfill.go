/**
* This file implements `platform-cli backfill` subcommand
 */

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/consumers"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/models"
	"github.com/xrpscan/platform/signals"
)

const BackfillCommandName = "backfill"
const defaultIndexFrom int = 82000000
const defaultIndexTo int = 82001000
const defaultMinDelay int64 = 100 // milliseconds

// getMinDelay gets the minimum delay from environment variable or uses default
func getMinDelay(defaultDelay int64) int64 {
	if envDelay := os.Getenv("BACKFILL_MIN_DELAY_MS"); envDelay != "" {
		if delay, err := strconv.ParseInt(envDelay, 10, 64); err == nil && delay > 0 {
			return delay
		}
	}
	return defaultDelay
}

type BackfillCommand struct {
	fs           *flag.FlagSet
	fConfigFile  string
	fXrplServer  string
	fIndexFrom   int
	fIndexTo     int
	fMinDelay    int64
	fVerbose     bool
	fLedgers     string // Comma-separated list of ledger indices to process
	fLedgersFile string // Path to file containing ledger indices (one per line)
}

func NewBackfillCommand() *BackfillCommand {
	cmd := &BackfillCommand{
		fs: flag.NewFlagSet(BackfillCommandName, flag.ExitOnError),
	}

	cmd.fs.IntVar(&cmd.fIndexFrom, "from", defaultIndexFrom, "From ledger index")
	cmd.fs.IntVar(&cmd.fIndexTo, "to", defaultIndexTo, "To ledger index")
	cmd.fs.StringVar(&cmd.fConfigFile, "config", ".env", "Environment config file")
	cmd.fs.BoolVar(&cmd.fVerbose, "verbose", false, "Make the command more talkative")
	cmd.fs.StringVar(&cmd.fXrplServer, "server", "", "XRPL protocol compatible server to connect")
	cmd.fs.Int64Var(&cmd.fMinDelay, "delay", getMinDelay(defaultMinDelay), "Minimum delay (ms) between requests to XRPL server (can be overridden by BACKFILL_MIN_DELAY_MS env var)")
	cmd.fs.StringVar(&cmd.fLedgers, "ledgers", "", "Comma-separated list of ledger indices to process (overrides --from and --to)")
	cmd.fs.StringVar(&cmd.fLedgersFile, "ledgers-file", "", "Path to file containing ledger indices to process, one per line (overrides --from and --to)")
	return cmd
}

func (cmd *BackfillCommand) Init(args []string) error {
	err := cmd.fs.Parse(args)
	if err != nil {
		return err
	}

	return cmd.Validate()
}

func (cmd *BackfillCommand) Validate() error {
	// If ledgers list or file is provided, --from and --to are ignored
	if cmd.fLedgers != "" || cmd.fLedgersFile != "" {
		return nil
	}

	// Ledgers are backfilled in chronological order. Therefore, --from ledger
	// index must be less than --to ledger index.
	if cmd.fIndexFrom > cmd.fIndexTo {
		return fmt.Errorf("from ledger (%d) must be less than to ledger (%d)", cmd.fIndexFrom, cmd.fIndexTo)
	}
	return nil
}

func (cmd *BackfillCommand) Name() string {
	return cmd.fs.Name()
}

// parseLedgersList parses a comma-separated list of ledger indices
func (cmd *BackfillCommand) parseLedgersList(ledgersStr string) ([]int, error) {
	parts := strings.Split(ledgersStr, ",")
	ledgers := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		ledgerIndex, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid ledger index '%s': %w", part, err)
		}
		ledgers = append(ledgers, ledgerIndex)
	}
	return ledgers, nil
}

// parseLedgersFile reads ledger indices from a file (one per line)
func (cmd *BackfillCommand) parseLedgersFile(filePath string) ([]int, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read ledgers file: %w", err)
	}

	lines := strings.Split(string(data), "\n")
	ledgers := make([]int, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue // Skip empty lines and comments
		}
		ledgerIndex, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("invalid ledger index '%s' in file: %w", line, err)
		}
		ledgers = append(ledgers, ledgerIndex)
	}
	return ledgers, nil
}

func (cmd *BackfillCommand) Run() error {
	// Register command line signal handlers to gracefully shutdown cli
	signals.HandleAll()

	// Load validated config file
	config.EnvLoad(cmd.fConfigFile)

	// If websocket url is not provided in the cli, use the url from environment
	if cmd.fXrplServer == "" {
		cmd.fXrplServer = config.EnvXrplWebsocketFullHistoryURL()
	}

	// Initialize connections to services
	logger.New()

	// Parse ledger list if provided
	var ledgersToProcess []int
	var useLedgerList bool

	if cmd.fLedgersFile != "" {
		var err error
		ledgersToProcess, err = cmd.parseLedgersFile(cmd.fLedgersFile)
		if err != nil {
			return fmt.Errorf("failed to parse ledgers file: %w", err)
		}
		useLedgerList = true
		log.Printf("[BACKFILL] Loaded %d ledgers from file: %s", len(ledgersToProcess), cmd.fLedgersFile)
	} else if cmd.fLedgers != "" {
		var err error
		ledgersToProcess, err = cmd.parseLedgersList(cmd.fLedgers)
		if err != nil {
			return fmt.Errorf("failed to parse ledgers list: %w", err)
		}
		useLedgerList = true
		log.Printf("[BACKFILL] Processing %d ledgers from list", len(ledgersToProcess))
	} else {
		log.Printf("[BACKFILL] Starting backfill process from ledger %d to %d", cmd.fIndexFrom, cmd.fIndexTo)
	}

	log.Printf("[BACKFILL] Using XRPL server: %s", cmd.fXrplServer)
	log.Printf("[BACKFILL] Minimum delay between requests: %dms", cmd.fMinDelay)

	log.Printf("[BACKFILL] Initializing ClickHouse connection...")
	connections.NewClickHouseConnection()
	log.Printf("[BACKFILL] ClickHouse connection initialized successfully")

	log.Printf("[BACKFILL] Initializing XRPL client...")
	connections.NewXrplClientWithURL(cmd.fXrplServer)
	log.Printf("[BACKFILL] XRPL client initialized successfully")

	log.Printf("[BACKFILL] Initializing XRPL RPC client...")
	connections.NewXrplRPCClientWithURL(cmd.fXrplServer)
	log.Printf("[BACKFILL] XRPL RPC client initialized successfully")

	// Consumers are no longer needed - transactions are processed directly

	defer func() {
		log.Printf("[BACKFILL] Closing remaining connections...")
		// Writer and readers are already closed before return
		connections.CloseClickHouse()
		connections.CloseXrplClient()
		connections.CloseXrplRPCClient()
		log.Printf("[BACKFILL] All connections closed")
	}()

	// Process ledgers
	var totalLedgers int
	if useLedgerList {
		totalLedgers = len(ledgersToProcess)
		log.Printf("[BACKFILL] Processing %d ledgers from list", totalLedgers)
		log.Printf("[BACKFILL] Starting sequential backfill process - will retry each ledger until success")

		for i, ledgerIndex := range ledgersToProcess {
			progress := float64(i+1) / float64(totalLedgers) * 100

			// Log progress every 100 ledgers or for first/last ledger
			if i == 0 || i == totalLedgers-1 || (i+1)%100 == 0 {
				log.Printf("[BACKFILL] Progress: %.1f%% (%d/%d) - Processing ledger %d",
					progress, i+1, totalLedgers, ledgerIndex)
			}

			// Process ledger with infinite retry until success
			cmd.processLedgerWithInfiniteRetry(ledgerIndex)
		}
	} else {
		// Fetch ledger and queue transactions for indexing
		totalLedgers = cmd.fIndexTo - cmd.fIndexFrom + 1
		log.Printf("[BACKFILL] Processing %d ledgers total", totalLedgers)
		log.Printf("[BACKFILL] Starting sequential backfill process - will retry each ledger until success")

		// Sequential processing loop - no skipping allowed
		for ledgerIndex := cmd.fIndexFrom; ledgerIndex <= cmd.fIndexTo; ledgerIndex++ {
			progress := float64(ledgerIndex-cmd.fIndexFrom+1) / float64(totalLedgers) * 100

			// Log progress every 100 ledgers or for first/last ledger
			if ledgerIndex == cmd.fIndexFrom || ledgerIndex == cmd.fIndexTo || ledgerIndex%100 == 0 {
				log.Printf("[BACKFILL] Progress: %.1f%% (%d/%d) - Processing ledger %d",
					progress, ledgerIndex-cmd.fIndexFrom+1, totalLedgers, ledgerIndex)
			}

			// Process ledger with infinite retry until success
			cmd.processLedgerWithInfiniteRetry(ledgerIndex)
		}
	}

	log.Printf("[BACKFILL] Backfill process completed successfully")

	// Flush all pending ClickHouse batches before closing connections
	log.Printf("[BACKFILL] Flushing all ClickHouse batches...")
	if err := connections.FlushClickHouse(); err != nil {
		log.Printf("[BACKFILL] Error flushing ClickHouse: %v", err)
	} else {
		log.Printf("[BACKFILL] ClickHouse batches flushed successfully")
	}

	return nil
}

// Worker functions

// processLedgerWithInfiniteRetry processes a single ledger with infinite retry until success
func (cmd *BackfillCommand) processLedgerWithInfiniteRetry(ledgerIndex int) {
	retryDelay := time.Second
	maxRetryDelay := 30 * time.Second
	attempt := 0

	for {
		attempt++
		startTime := time.Now()

		log.Printf("[BACKFILL] Starting attempt %d for ledger %d", attempt, ledgerIndex)

		err := cmd.backfillLedgerWithRetry(ledgerIndex)
		reqDuration := time.Since(startTime)

		if err == nil {
			// Success!
			log.Printf("[BACKFILL] Successfully processed ledger %d in %v (attempt %d)",
				ledgerIndex, reqDuration, attempt)

			// Honor fair usage policy and wait before sending next request
			delayRequired := cmd.fMinDelay - reqDuration.Milliseconds()
			if delayRequired > 0 {
				time.Sleep(time.Duration(delayRequired) * time.Millisecond)
			}

			return // Exit function on success
		}

		log.Printf("[BACKFILL] Error backfilling ledger %d (attempt %d, duration %v): %v",
			ledgerIndex, attempt, reqDuration, err)

		// Check if it's a timeout error (request took too long)
		if reqDuration > 35*time.Second {
			log.Printf("[BACKFILL] Request for ledger %d took too long (%v), likely hung",
				ledgerIndex, reqDuration)
		}

		// Check if it's a WebSocket connection error
		if isWebSocketConnectionError(err) {
			log.Printf("[BACKFILL] WebSocket connection error detected for ledger %d, attempting reconnection", ledgerIndex)

			// Check if it's specifically an IP limit error
			if isIPLimitError(err) {
				log.Printf("[BACKFILL] IP limit reached error detected for ledger %d", ledgerIndex)
				log.Printf("[BACKFILL] Server is blocking connections from this IP address")
				log.Printf("[BACKFILL] Waiting longer before retry to allow IP limit to reset...")

				// Wait longer for IP limit to reset
				time.Sleep(5 * time.Minute)
			} else {
				// Try to reconnect XRPL clients with infinite retry
				log.Printf("[BACKFILL] Attempting to reconnect XRPL clients...")
				connections.CloseXrplClient()
				connections.CloseXrplRPCClient()
				connections.NewXrplClientWithURL(cmd.fXrplServer)
				connections.NewXrplRPCClientWithURL(cmd.fXrplServer)
				log.Printf("[BACKFILL] XRPL clients reconnected")
			}
		}

		log.Printf("[BACKFILL] Retrying ledger %d in %v...", ledgerIndex, retryDelay)
		time.Sleep(retryDelay)

		// Exponential backoff
		if retryDelay < maxRetryDelay {
			retryDelay *= 2
			if retryDelay > maxRetryDelay {
				retryDelay = maxRetryDelay
			}
		}
	}
}

// backfillLedgerWithRetry attempts to backfill a ledger with error handling and retry logic
func (cmd *BackfillCommand) backfillLedgerWithRetry(ledgerIndex int) error {
	if cmd.fVerbose {
		log.Printf("[BACKFILL] Processing ledger: %d", ledgerIndex)
	}

	ledger := models.LedgerStream{
		Type:        models.LEDGER_STREAM_TYPE,
		LedgerIndex: uint32(ledgerIndex),
	}

	ledgerJSON, err := json.Marshal(ledger)
	if err != nil {
		return fmt.Errorf("failed to marshal ledger: %w", err)
	}

	// Ledger production to Kafka is no longer needed - transactions are processed directly

	// Try to backfill transactions
	if cmd.fVerbose {
		log.Printf("[BACKFILL] Fetching transactions for ledger %d...", ledgerIndex)
	}

	// Log start time for transaction fetching
	startTime := time.Now()
	err = cmd.backfillTransactionsWithRetry(ledgerJSON)
	requestDuration := time.Since(startTime)

	if err != nil {
		log.Printf("[BACKFILL] Failed to fetch transactions for ledger %d after %v: %v",
			ledgerIndex, requestDuration, err)
		return fmt.Errorf("failed to backfill transactions: %w", err)
	}
	if cmd.fVerbose {
		log.Printf("[BACKFILL] Transactions for ledger %d processed successfully in %v",
			ledgerIndex, requestDuration)
	}

	return nil
}

// backfillTransactionsWithRetry attempts to backfill transactions with retry logic
func (cmd *BackfillCommand) backfillTransactionsWithRetry(ledgerJSON []byte) error {
	maxRetries := 3
	retryDelay := time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		err := cmd.backfillTransactions(ledgerJSON)
		if err == nil {
			return nil // Success
		}

		log.Printf("[BACKFILL] Error backfilling transactions (attempt %d/%d): %v",
			attempt+1, maxRetries, err)

		if attempt < maxRetries-1 {
			log.Printf("[BACKFILL] Retrying transaction backfill in %v...", retryDelay)
			time.Sleep(retryDelay)
			retryDelay *= 2

			// Try to reconnect XRPL RPC client with infinite retry
			log.Printf("[BACKFILL] Attempting to reconnect XRPL RPC client...")
			if err := connections.ReconnectXRPLRPCClient(cmd.fXrplServer); err != nil {
				log.Printf("[BACKFILL] Failed to reconnect XRPL RPC client: %v", err)
			} else {
				log.Printf("[BACKFILL] XRPL RPC client reconnected successfully")
			}
		}
	}

	return fmt.Errorf("failed to backfill transactions after %d attempts", maxRetries)
}

func (cmd *BackfillCommand) backfillTransactions(ledgerJSON []byte) error {
	var ledger models.LedgerStream
	if err := json.Unmarshal(ledgerJSON, &ledger); err != nil {
		return fmt.Errorf("JSON unmarshal error: %w", err)
	}

	// Fetch all transactions included in this ledger from XRPL server
	if cmd.fVerbose {
		log.Printf("[BACKFILL] Fetching transactions from XRPL server for ledger %d...", ledger.LedgerIndex)
	}
	txResponse, err := ledger.FetchTransactions()
	if err != nil {
		return fmt.Errorf("failed to fetch transactions for ledger %d: %w", ledger.LedgerIndex, err)
	}

	// Verify if result.ledger.transactions property is present
	txResult, ok := txResponse["result"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("tx response has no result property for ledger %d", ledger.LedgerIndex)
	}
	txLedger, ok := txResult["ledger"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("tx response has no result.ledger property for ledger %d", ledger.LedgerIndex)
	}
	txs, ok := txLedger["transactions"].([]interface{})
	if !ok {
		return fmt.Errorf("tx response has no result.ledger.transactions property for ledger %d", ledger.LedgerIndex)
	}

	// Type assert ledger_index and date fields
	ledgerIndexStr, ok := txLedger["ledger_index"].(string)
	if !ok {
		return fmt.Errorf("ledger has invalid ledger_index property for ledger %d", ledger.LedgerIndex)
	}
	ledgerIndex, err := strconv.Atoi(ledgerIndexStr)
	if err != nil {
		return fmt.Errorf("cannot convert ledger_index to int for ledger %d: %w", ledger.LedgerIndex, err)
	}
	closeTime, ok := txLedger["close_time"].(float64)
	if !ok {
		return fmt.Errorf("ledger has invalid close_time property for ledger %d", ledger.LedgerIndex)
	}

	// Process transactions directly and write to ClickHouse
	processedCount := 0
	skippedCount := 0
	errorCount := 0

	log.Printf("[BACKFILL] Processing %d transactions for ledger %d", len(txs), ledger.LedgerIndex)

	for _, txo := range txs {
		tx, ok := txo.(map[string]interface{})
		if !ok {
			return fmt.Errorf("error asserting transaction type for ledger %d", ledger.LedgerIndex)
		}

		// Transactions fetched by `ledger` command do not have date, validated,
		// ledger_index fields. Populating these tx fields from ledger data.
		// Use float64 for ledger_index to match XRPL format
		tx["ledger_index"] = float64(ledgerIndex)
		tx["date"] = closeTime
		tx["validated"] = true

		var base map[string]interface{} = tx
		hash, _ := base["hash"].(string)
		if hash == "" {
			skippedCount++
			// Only log if detailed logging is enabled for this ledger
			if config.ShouldLogDetailed(ledger.LedgerIndex) {
				logger.Log.Debug().
					Uint32("ledger_index", ledger.LedgerIndex).
					Msg("Skipping transaction without hash")
			}
			continue
		}

		// Check transaction type before processing
		txType, _ := base["TransactionType"].(string)
		if txType != "Payment" {
			skippedCount++
			// Only log if detailed logging is enabled for this ledger
			if cmd.fVerbose && config.ShouldLogDetailed(ledger.LedgerIndex) {
				logger.Log.Debug().
					Str("tx_hash", hash).
					Uint32("ledger_index", ledger.LedgerIndex).
					Str("transaction_type", txType).
					Msg("Skipping non-Payment transaction")
			}
			continue
		}

		// Process transaction directly and write to ClickHouse
		if err := consumers.ProcessTransaction(tx); err != nil {
			errorCount++
			logger.Log.Error().
				Err(err).
				Str("tx_hash", hash).
				Uint32("ledger_index", ledger.LedgerIndex).
				Msg("Failed to process transaction")
			// Continue processing other transactions even if one fails
		} else {
			processedCount++
			// Only log if detailed logging is enabled for this ledger
			if cmd.fVerbose && config.ShouldLogDetailed(ledger.LedgerIndex) {
				logger.Log.Debug().
					Str("tx_hash", hash).
					Uint32("ledger_index", ledger.LedgerIndex).
					Msg("Transaction processed successfully")
			}
		}
	}

	log.Printf("[BACKFILL] Ledger %d processing summary: total=%d, processed=%d, skipped=%d, errors=%d",
		ledger.LedgerIndex, len(txs), processedCount, skippedCount, errorCount)

	// If no Payment transactions were processed, record this ledger as empty
	if processedCount == 0 {
		if err := connections.RecordEmptyLedger(uint32(ledgerIndex), int64(closeTime), uint32(len(txs))); err != nil {
			log.Printf("[BACKFILL] Failed to record empty ledger %d: %v", ledgerIndex, err)
			// Don't fail the whole process if recording empty ledger fails
		} else {
			log.Printf("[BACKFILL] Recorded ledger %d as empty (no Payment transactions, total txs: %d)",
				ledgerIndex, len(txs))
		}
	}

	return nil
}

// isWebSocketConnectionError checks if the error is related to WebSocket connection issues
func isWebSocketConnectionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "websocket") ||
		strings.Contains(errStr, "close 1006") ||
		strings.Contains(errStr, "close 1008") ||
		strings.Contains(errStr, "policy violation") ||
		strings.Contains(errStr, "IP limit reached") ||
		strings.Contains(errStr, "unexpected EOF") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection refused")
}

// isIPLimitError checks if the error is specifically about IP limit reached
func isIPLimitError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "close 1008") ||
		strings.Contains(errStr, "policy violation") ||
		strings.Contains(errStr, "IP limit reached")
}

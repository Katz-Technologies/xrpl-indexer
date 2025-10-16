/**
* This file implements `platform-cli backfill` subcommand
 */

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
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

type BackfillCommand struct {
	fs          *flag.FlagSet
	fConfigFile string
	fXrplServer string
	fIndexFrom  int
	fIndexTo    int
	fMinDelay   int64
	fVerbose    bool
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
	cmd.fs.Int64Var(&cmd.fMinDelay, "delay", defaultMinDelay, "Minimum delay (ms) between requests to XRPL server")
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

	log.Printf("[BACKFILL] Starting backfill process from ledger %d to %d", cmd.fIndexFrom, cmd.fIndexTo)
	log.Printf("[BACKFILL] Using XRPL server: %s", cmd.fXrplServer)
	log.Printf("[BACKFILL] Minimum delay between requests: %dms", cmd.fMinDelay)

	log.Printf("[BACKFILL] Initializing Kafka writer...")
	connections.NewWriter()
	log.Printf("[BACKFILL] Kafka writer initialized successfully")

	log.Printf("[BACKFILL] Initializing Kafka readers...")
	connections.NewReaders()
	log.Printf("[BACKFILL] Kafka readers initialized successfully")

	log.Printf("[BACKFILL] Initializing XRPL client...")
	connections.NewXrplClientWithURL(cmd.fXrplServer)
	log.Printf("[BACKFILL] XRPL client initialized successfully")

	log.Printf("[BACKFILL] Initializing XRPL RPC client...")
	connections.NewXrplRPCClientWithURL(cmd.fXrplServer)
	log.Printf("[BACKFILL] XRPL RPC client initialized successfully")

	log.Printf("[BACKFILL] Starting consumers to process data for ClickHouse...")
	go consumers.RunConsumers()
	log.Printf("[BACKFILL] Consumers started successfully")

	// Give consumers time to initialize
	log.Printf("[BACKFILL] Waiting for consumers to initialize...")
	time.Sleep(2 * time.Second)
	log.Printf("[BACKFILL] Consumers initialization complete")

	defer func() {
		log.Printf("[BACKFILL] Closing connections...")
		connections.CloseWriter()
		connections.CloseReaders()
		connections.CloseXrplClient()
		connections.CloseXrplRPCClient()
		log.Printf("[BACKFILL] All connections closed")
	}()

	// Fetch ledger and queue transactions for indexing
	totalLedgers := cmd.fIndexTo - cmd.fIndexFrom + 1
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

	log.Printf("[BACKFILL] Backfill process completed successfully")
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

	// Try to produce ledger to Kafka
	if cmd.fVerbose {
		log.Printf("[BACKFILL] Producing ledger %d to Kafka...", ledgerIndex)
	}
	err = cmd.produceLedgerWithRetry(ledgerJSON)
	if err != nil {
		return fmt.Errorf("failed to produce ledger: %w", err)
	}
	if cmd.fVerbose {
		log.Printf("[BACKFILL] Ledger %d produced to Kafka successfully", ledgerIndex)
	}

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

// produceLedgerWithRetry attempts to produce ledger to Kafka with retry logic
func (cmd *BackfillCommand) produceLedgerWithRetry(ledgerJSON []byte) error {
	maxRetries := 3
	retryDelay := time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		err := cmd.produceLedger(ledgerJSON)
		if err == nil {
			return nil // Success
		}

		log.Printf("[BACKFILL] Error producing ledger to Kafka (attempt %d/%d): %v",
			attempt+1, maxRetries, err)

		if attempt < maxRetries-1 {
			log.Printf("[BACKFILL] Retrying ledger production in %v...", retryDelay)
			time.Sleep(retryDelay)
			retryDelay *= 2
		}
	}

	return fmt.Errorf("failed to produce ledger after %d attempts", maxRetries)
}

// produceLedger produces a single ledger to Kafka
func (cmd *BackfillCommand) produceLedger(ledgerJSON []byte) error {
	var res map[string]interface{}
	if err := json.Unmarshal(ledgerJSON, &res); err != nil {
		return fmt.Errorf("failed to unmarshal ledger: %w", err)
	}

	messageKey := strconv.Itoa(int(res["ledger_index"].(float64)))

	err := connections.KafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Topic: config.TopicLedgers(),
			Key:   []byte(messageKey),
			Value: ledgerJSON,
		},
	)
	if err != nil {
		return fmt.Errorf("kafka write failed: %w", err)
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

	// Build a batch of kafka messages and write in one call
	msgs := make([]kafka.Message, 0, len(txs))
	for _, txo := range txs {
		tx, ok := txo.(map[string]interface{})
		if !ok {
			return fmt.Errorf("error asserting transaction type for ledger %d", ledger.LedgerIndex)
		}

		// Transactions fetched by `ledger` command do not have date, validated and
		// ledger_index fields. Populating these tx fields from ledger data.
		tx["ledger_index"] = ledgerIndex
		tx["date"] = closeTime
		tx["validated"] = true

		var base map[string]interface{} = tx
		hash, _ := base["hash"].(string)
		if hash == "" {
			// Skip malformed tx
			continue
		}

		txJSON, err := json.Marshal(tx)
		if err != nil {
			return fmt.Errorf("error marshaling transaction for ledger %d: %w", ledger.LedgerIndex, err)
		}
		msgs = append(msgs, kafka.Message{
			Topic: config.TopicTransactions(),
			Key:   []byte(hash),
			Value: txJSON,
		})
	}

	if len(msgs) > 0 {
		if cmd.fVerbose {
			log.Printf("[BACKFILL] Writing %d transactions to Kafka for ledger %d...", len(msgs), ledger.LedgerIndex)
		}
		if err := connections.KafkaWriter.WriteMessages(context.Background(), msgs...); err != nil {
			return fmt.Errorf("failed to produce transaction batch for ledger %d: %w", ledger.LedgerIndex, err)
		}
		if cmd.fVerbose {
			log.Printf("[BACKFILL] Successfully wrote %d transactions to Kafka for ledger %d", len(msgs), ledger.LedgerIndex)
		}
	} else {
		if cmd.fVerbose {
			log.Printf("[BACKFILL] No transactions found for ledger %d", ledger.LedgerIndex)
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

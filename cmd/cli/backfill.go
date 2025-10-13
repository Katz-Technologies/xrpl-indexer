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
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
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
	connections.NewWriter()
	connections.NewXrplClientWithURL(cmd.fXrplServer)
	connections.NewXrplRPCClientWithURL(cmd.fXrplServer)
	defer connections.CloseWriter()
	defer connections.CloseXrplClient()
	defer connections.CloseXrplRPCClient()

	// Fetch ledger and queue transactions for indexing
	for ledgerIndex := cmd.fIndexFrom; ledgerIndex <= cmd.fIndexTo; ledgerIndex++ {
		startTime := time.Now().UnixNano() / int64(time.Millisecond)

		// Retry logic for each ledger with connection recovery
		maxRetries := 3
		retryDelay := time.Second

		for attempt := 0; attempt < maxRetries; attempt++ {
			err := cmd.backfillLedgerWithRetry(ledgerIndex)
			if err == nil {
				break // Success, move to next ledger
			}

			log.Printf("[%s] Error backfilling ledger %d (attempt %d/%d): %v",
				cmd.fXrplServer, ledgerIndex, attempt+1, maxRetries, err)

			if attempt < maxRetries-1 {
				log.Printf("[%s] Retrying ledger %d in %v...",
					cmd.fXrplServer, ledgerIndex, retryDelay)
				time.Sleep(retryDelay)
				retryDelay *= 2 // Exponential backoff

				// Try to reconnect XRPL clients
				log.Printf("[%s] Attempting to reconnect XRPL clients...", cmd.fXrplServer)
				connections.CloseXrplClient()
				connections.CloseXrplRPCClient()
				connections.NewXrplClientWithURL(cmd.fXrplServer)
				connections.NewXrplRPCClientWithURL(cmd.fXrplServer)
			} else {
				log.Printf("[%s] Failed to backfill ledger %d after %d attempts, skipping",
					cmd.fXrplServer, ledgerIndex, maxRetries)
			}
		}

		reqDuration := time.Now().UnixNano()/int64(time.Millisecond) - startTime

		// Honor fair usage policy and wait before sending next request
		delayRequired := cmd.fMinDelay - reqDuration
		if delayRequired > 0 {
			time.Sleep(time.Duration(delayRequired) * time.Millisecond)
		}
	}
	return nil
}

// Worker functions

// backfillLedgerWithRetry attempts to backfill a ledger with error handling and retry logic
func (cmd *BackfillCommand) backfillLedgerWithRetry(ledgerIndex int) error {
	log.Printf("[%s] Backfilling ledger: %d", cmd.fXrplServer, ledgerIndex)

	ledger := models.LedgerStream{
		Type:        models.LEDGER_STREAM_TYPE,
		LedgerIndex: uint32(ledgerIndex),
	}

	ledgerJSON, err := json.Marshal(ledger)
	if err != nil {
		return fmt.Errorf("failed to marshal ledger: %w", err)
	}

	// Try to produce ledger to Kafka
	err = cmd.produceLedgerWithRetry(ledgerJSON)
	if err != nil {
		return fmt.Errorf("failed to produce ledger: %w", err)
	}

	// Try to backfill transactions
	err = cmd.backfillTransactionsWithRetry(ledgerJSON)
	if err != nil {
		return fmt.Errorf("failed to backfill transactions: %w", err)
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

		log.Printf("[%s] Error producing ledger (attempt %d/%d): %v",
			cmd.fXrplServer, attempt+1, maxRetries, err)

		if attempt < maxRetries-1 {
			log.Printf("[%s] Retrying ledger production in %v...",
				cmd.fXrplServer, retryDelay)
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

		log.Printf("[%s] Error backfilling transactions (attempt %d/%d): %v",
			cmd.fXrplServer, attempt+1, maxRetries, err)

		if attempt < maxRetries-1 {
			log.Printf("[%s] Retrying transaction backfill in %v...",
				cmd.fXrplServer, retryDelay)
			time.Sleep(retryDelay)
			retryDelay *= 2

			// Try to reconnect XRPL RPC client
			log.Printf("[%s] Attempting to reconnect XRPL RPC client...", cmd.fXrplServer)
			connections.CloseXrplRPCClient()
			connections.NewXrplRPCClientWithURL(cmd.fXrplServer)
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
		if err := connections.KafkaWriter.WriteMessages(context.Background(), msgs...); err != nil {
			return fmt.Errorf("failed to produce transaction batch for ledger %d: %w", ledger.LedgerIndex, err)
		}
	}

	return nil
}

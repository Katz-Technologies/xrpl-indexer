package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/consumers"
	"github.com/xrpscan/platform/ipc"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/routes"
	"github.com/xrpscan/platform/socketio"
)

type IndexerOrchestrator struct {
	config          *Config
	workers         []*IndexerWorker
	processedHashes map[string]bool
	hashMutex       sync.RWMutex
	chInitialized   bool
	// Group transactions by ledger for token detection
	ledgerTransactions map[uint32][]map[string]interface{}
	ledgerTimestamps   map[uint32]time.Time
	ledgerMutex        sync.Mutex
}

func NewIndexerOrchestrator(cfg *Config) (*IndexerOrchestrator, error) {
	// Ensure logs directory exists
	if err := os.MkdirAll("logs", 0755); err != nil {
		return nil, fmt.Errorf("failed to create logs directory: %w", err)
	}

	// Verify server path exists
	if _, err := os.Stat(cfg.ServerPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("server executable not found at %s", cfg.ServerPath)
	}

	return &IndexerOrchestrator{
		config:             cfg,
		workers:            make([]*IndexerWorker, 0, cfg.Workers),
		processedHashes:    make(map[string]bool),
		ledgerTransactions: make(map[uint32][]map[string]interface{}),
		ledgerTimestamps:   make(map[uint32]time.Time),
	}, nil
}

func (o *IndexerOrchestrator) Run(ctx context.Context, cancel context.CancelFunc) error {
	log.Printf("[INDEXER-ORCHESTRATOR] Starting orchestrator with %d workers", o.config.Workers)
	log.Printf("[INDEXER-ORCHESTRATOR] Server path: %s", o.config.ServerPath)
	log.Printf("[INDEXER-ORCHESTRATOR] Servers: %v", o.config.Servers)
	log.Printf("[INDEXER-ORCHESTRATOR] Check interval: %v", o.config.CheckInterval)

	// Initialize ClickHouse connection once for the entire orchestrator
	if !o.chInitialized {
		config.EnvLoad(o.config.ConfigFile)
		logger.New()
		connections.NewClickHouseConnection()

		// Set callback to check tokens after batch flush
		connections.SetBatchFlushCallback(o.onBatchFlush)

		// Initialize XRPL RPC client for token checking
		// Use third server (xrplcluster.com) for orchestrator's API calls
		// This distributes load: workers use s1/s2, orchestrator uses cluster
		orchestratorRPCURL := "wss://xrplcluster.com/"
		if len(o.config.Servers) >= 3 {
			orchestratorRPCURL = o.config.Servers[2] // Use third server if available
		} else if len(o.config.Servers) > 0 {
			// Fallback to last server if less than 3 servers
			orchestratorRPCURL = o.config.Servers[len(o.config.Servers)-1]
		}
		log.Printf("[INDEXER-ORCHESTRATOR] Using XRPL RPC URL for token checking: %s", orchestratorRPCURL)
		// Use NewXrplRPCClientWithURL directly to bypass environment variables
		// This ensures the RPC client uses the orchestrator's URL, not the one from .env

		// Initialize SocketIO hub for notifications
		socketio.GetHub()

		// Start HTTP server for SocketIO connections
		go o.startHTTPServer()

		o.chInitialized = true
		log.Printf("[INDEXER-ORCHESTRATOR] ClickHouse connection initialized")
		log.Printf("[INDEXER-ORCHESTRATOR] Batch flush callback set for token detection")
		log.Printf("[INDEXER-ORCHESTRATOR] XRPL RPC client initialized with URL: %s", orchestratorRPCURL)
		log.Printf("[INDEXER-ORCHESTRATOR] SocketIO hub initialized")
	}

	// Setup signal handlers for graceful shutdown
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-stopChan
		log.Printf("[INDEXER-ORCHESTRATOR] Received signal: %v, shutting down gracefully...", sig)
		o.StopAllWorkers(true)
		if o.chInitialized {
			connections.CloseClickHouse()
		}
		cancel()
	}()

	// Close ClickHouse connection on exit
	defer func() {
		if o.chInitialized {
			connections.CloseClickHouse()
		}
	}()

	// Start workers
	if err := o.startWorkers(); err != nil {
		return fmt.Errorf("failed to start workers: %w", err)
	}

	// Main orchestration loop
	for {
		select {
		case <-ctx.Done():
			log.Printf("[INDEXER-ORCHESTRATOR] Context cancelled, shutting down...")
			o.StopAllWorkers(true)
			return nil
		default:
			// Check worker status and restart if needed
			o.checkWorkers()

			// Sleep before next check
			time.Sleep(o.config.CheckInterval)
		}
	}
}

func (o *IndexerOrchestrator) startWorkers() error {
	log.Printf("[INDEXER-ORCHESTRATOR] Starting %d indexer workers", o.config.Workers)
	log.Printf("[INDEXER-ORCHESTRATOR] Servers: %v", o.config.Servers)

	o.workers = make([]*IndexerWorker, 0, o.config.Workers)

	for i := 0; i < o.config.Workers; i++ {
		// Distribute servers using round-robin (like in backfill orchestrator)
		server := o.config.Servers[i%len(o.config.Servers)]

		worker, err := NewIndexerWorker(
			i+1,
			o.config.ServerPath,
			o.config.ConfigFile,
			server,
		)
		if err != nil {
			o.StopAllWorkers(false)
			return fmt.Errorf("failed to create worker %d: %w", i+1, err)
		}

		if err := worker.Start(); err != nil {
			o.StopAllWorkers(false)
			return fmt.Errorf("failed to start worker %d: %w", i+1, err)
		}

		o.workers = append(o.workers, worker)

		// Start goroutine to handle IPC communication for this worker
		go o.handleWorkerIPC(worker)

		log.Printf("[INDEXER-ORCHESTRATOR] Started worker %d with PID %d on server %s", worker.ID, worker.PID(), server)
	}

	return nil
}

func (o *IndexerOrchestrator) handleWorkerIPC(worker *IndexerWorker) {
	// Note: In subprocess mode, we read from stdout and write to stdin
	// The worker's stdout is what the subprocess writes to, and stdin is what we write to
	protocol := ipc.NewScannerProtocol(worker.Stdin(), worker.Stdout())

	// Read events from worker
	for protocol.Scan() {
		eventType, err := protocol.DetectEventType()
		if err != nil {
			// Skip non-JSON lines (likely log output that shouldn't be in stdout)
			// This should not happen if logs are properly redirected to stderr
			continue
		}

		switch eventType {
		case ipc.EventTypeReady:
			// Worker is ready
			event, err := protocol.Event()
			if err != nil {
				log.Printf("[INDEXER-ORCHESTRATOR] Error decoding ready event from worker %d: %v", worker.ID, err)
				continue
			}
			log.Printf("[INDEXER-ORCHESTRATOR] Worker %d is ready: %v", worker.ID, event.Data)

		case ipc.EventTypeBatch:
			// Handle batch of transactions
			batchEvent, err := protocol.BatchEvent()
			if err != nil {
				log.Printf("[INDEXER-ORCHESTRATOR] Error decoding batch event from worker %d: %v", worker.ID, err)
				continue
			}
			o.processBatch(batchEvent.Items, worker.ID)

		case ipc.EventTypeData:
			// Handle single transaction
			event, err := protocol.Event()
			if err != nil {
				log.Printf("[INDEXER-ORCHESTRATOR] Error decoding event from worker %d: %v", worker.ID, err)
				continue
			}
			o.processTransaction(event.Data, worker.ID)

		case ipc.EventTypeError:
			// Handle error from worker
			event, err := protocol.Event()
			if err != nil {
				log.Printf("[INDEXER-ORCHESTRATOR] Error decoding error event from worker %d: %v", worker.ID, err)
				continue
			}
			log.Printf("[INDEXER-ORCHESTRATOR] Error from worker %d: %v", worker.ID, event.Data)

		case ipc.EventTypeDone:
			// Worker is done
			log.Printf("[INDEXER-ORCHESTRATOR] Worker %d sent done event", worker.ID)
		}
	}

	if err := protocol.Err(); err != nil {
		log.Printf("[INDEXER-ORCHESTRATOR] Error reading from worker %d: %v", worker.ID, err)
	}
}

func (o *IndexerOrchestrator) processBatch(items []interface{}, workerID int) {
	for _, item := range items {
		o.processTransaction(item, workerID)
	}
}

func (o *IndexerOrchestrator) processTransaction(data interface{}, workerID int) {
	// Convert data to transaction map
	txData, err := json.Marshal(data)
	if err != nil {
		log.Printf("[INDEXER-ORCHESTRATOR] Error marshaling transaction data from worker %d: %v", workerID, err)
		return
	}

	var tx map[string]interface{}
	if err := json.Unmarshal(txData, &tx); err != nil {
		log.Printf("[INDEXER-ORCHESTRATOR] Error unmarshaling transaction from worker %d: %v", workerID, err)
		return
	}

	// Extract hash
	hash, ok := tx["hash"].(string)
	if !ok || hash == "" {
		log.Printf("[INDEXER-ORCHESTRATOR] Transaction from worker %d has no hash, skipping", workerID)
		return
	}

	// Check for duplicates
	o.hashMutex.RLock()
	alreadyProcessed := o.processedHashes[hash]
	o.hashMutex.RUnlock()

	if alreadyProcessed {
		return
	}

	// Mark as processed
	o.hashMutex.Lock()
	o.processedHashes[hash] = true
	o.hashMutex.Unlock()

	// Process transaction
	_, _, err = consumers.ProcessTransaction(tx)
	if err != nil {
		log.Printf("[INDEXER-ORCHESTRATOR] Error processing transaction %s from worker %d: %v", hash, workerID, err)
		// Remove from processed hashes on error so it can be retried
		o.hashMutex.Lock()
		delete(o.processedHashes, hash)
		o.hashMutex.Unlock()
		return
	}

	// Group transactions by ledger for token detection (will be checked after batch flush)
	ledgerIndex, ok := getLedgerIndex(tx)
	if ok {
		o.ledgerMutex.Lock()
		o.ledgerTransactions[ledgerIndex] = append(o.ledgerTransactions[ledgerIndex], tx)

		// Store ledger timestamp if available
		if timestamp := getLedgerTimestamp(tx); !timestamp.IsZero() {
			o.ledgerTimestamps[ledgerIndex] = timestamp
		}
		o.ledgerMutex.Unlock()
		// Note: Token detection is now triggered after batch flush, not immediately
	}
}

// Helper functions to extract ledger info from transaction
func getLedgerIndex(tx map[string]interface{}) (uint32, bool) {
	if li, ok := tx["ledger_index"].(float64); ok {
		return uint32(li), true
	}
	if li, ok := tx["ledger_index"].(uint32); ok {
		return li, true
	}
	if li, ok := tx["ledger_index"].(int); ok {
		return uint32(li), true
	}
	if li, ok := tx["ledger_index"].(string); ok {
		if idx, err := strconv.Atoi(li); err == nil {
			return uint32(idx), true
		}
	}
	return 0, false
}

func getLedgerTimestamp(tx map[string]interface{}) time.Time {
	if date, ok := tx["date"].(float64); ok {
		const rippleToUnix int64 = 946684800
		return time.Unix(int64(date)+rippleToUnix, 0).UTC()
	}
	return time.Time{}
}

// onBatchFlush is called after a batch is flushed to ClickHouse
// It triggers token detection for all ledgers that were in the batch
func (o *IndexerOrchestrator) onBatchFlush(batch []connections.MoneyFlowRow) {
	if len(batch) == 0 {
		return
	}

	// Group money flows by ledger index
	ledgerFlows := make(map[uint32][]connections.MoneyFlowRow)
	for _, row := range batch {
		ledgerFlows[row.LedgerIndex] = append(ledgerFlows[row.LedgerIndex], row)
	}

	// Check tokens for each ledger in the batch
	for ledgerIndex, flows := range ledgerFlows {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		if err := connections.CheckAndNotifyNewTokens(ctx, flows, ledgerIndex); err != nil {
			log.Printf("[INDEXER-ORCHESTRATOR] Error checking tokens for ledger %d: %v", ledgerIndex, err)
		}
		cancel()
	}
}

// startHTTPServer starts HTTP server for SocketIO connections
func (o *IndexerOrchestrator) startHTTPServer() {
	e := echo.New()
	e.HideBanner = true
	routes.Add(e)

	serverAddress := fmt.Sprintf("%s:%s", config.EnvServerHost(), config.EnvServerPort())
	log.Printf("[INDEXER-ORCHESTRATOR] Starting HTTP server for SocketIO on %s", serverAddress)
	if err := e.Start(serverAddress); err != nil {
		log.Printf("[INDEXER-ORCHESTRATOR] HTTP server error: %v", err)
	}
}

func (o *IndexerOrchestrator) checkWorkers() {
	for _, worker := range o.workers {
		if err := worker.CheckStatus(); err != nil {
			log.Printf("[INDEXER-ORCHESTRATOR] Error checking worker %d: %v", worker.ID, err)
		}

		if worker.IsFailed() {
			log.Printf("[INDEXER-ORCHESTRATOR] Worker %d failed, restarting...", worker.ID)
			// Stop the worker first
			if err := worker.Stop(false); err != nil {
				log.Printf("[INDEXER-ORCHESTRATOR] Error stopping failed worker %d: %v", worker.ID, err)
			}
			// Cleanup and create new worker
			worker.Cleanup()

			// Create new worker instance with the same server URL
			newWorker, err := NewIndexerWorker(worker.ID, worker.serverPath, worker.configFile, worker.serverURL)
			if err != nil {
				log.Printf("[INDEXER-ORCHESTRATOR] Failed to create new worker %d: %v", worker.ID, err)
				continue
			}

			// Find and replace worker in slice
			for i, w := range o.workers {
				if w.ID == worker.ID {
					o.workers[i] = newWorker
					break
				}
			}

			// Start new worker
			if err := newWorker.Start(); err != nil {
				log.Printf("[INDEXER-ORCHESTRATOR] Failed to start new worker %d: %v", newWorker.ID, err)
			} else {
				// Start IPC handler for new worker
				go o.handleWorkerIPC(newWorker)
				log.Printf("[INDEXER-ORCHESTRATOR] Successfully restarted worker %d", newWorker.ID)
			}
		}
	}
}

func (o *IndexerOrchestrator) StopAllWorkers(graceful bool) {
	log.Printf("[INDEXER-ORCHESTRATOR] Stopping all workers (graceful=%v)...", graceful)
	for _, worker := range o.workers {
		if err := worker.Stop(graceful); err != nil {
			log.Printf("[INDEXER-ORCHESTRATOR] Error stopping worker %d: %v", worker.ID, err)
		}
	}
	log.Printf("[INDEXER-ORCHESTRATOR] All workers stopped")
}

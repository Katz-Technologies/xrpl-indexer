package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	// Parse configuration
	cfg, err := ParseConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing configuration: %v\n", err)
		os.Exit(1)
	}

	// Setup log file
	if cfg.LogFile != "" {
		logFile, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening log file: %v\n", err)
			os.Exit(1)
		}
		defer logFile.Close()

		// Set log output to both file and stdout
		log.SetOutput(logFile)
		log.Printf("Indexer Orchestrator logging to file: %s", cfg.LogFile)
	}

	// Verify server exists
	if _, err := os.Stat(cfg.ServerPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: server executable not found at %s\n", cfg.ServerPath)
		fmt.Fprintf(os.Stderr, "Please build the server first: make build\n")
		os.Exit(1)
	}

	// Create orchestrator
	orchestrator, err := NewIndexerOrchestrator(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating orchestrator: %v\n", err)
		os.Exit(1)
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run orchestrator
	log.Printf("[INDEXER-ORCHESTRATOR] Starting indexer orchestrator...")
	startTime := time.Now()

	if err := orchestrator.Run(ctx, cancel); err != nil {
		log.Printf("[INDEXER-ORCHESTRATOR] Orchestrator error: %v", err)
		os.Exit(1)
	}

	duration := time.Since(startTime)
	log.Printf("[INDEXER-ORCHESTRATOR] Orchestrator completed successfully in %v", duration)
}

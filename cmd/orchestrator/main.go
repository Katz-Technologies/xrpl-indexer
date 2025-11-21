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

	// Setup log file if specified
	if cfg.LogFile != "" {
		logFile, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening log file: %v\n", err)
			os.Exit(1)
		}
		defer logFile.Close()

		// Set log output to both file and stdout
		log.SetOutput(logFile)
		log.Printf("Orchestrator logging to file: %s", cfg.LogFile)
	}

	// Verify CLI exists
	if err := VerifyCLIExists(cfg.CLIPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		fmt.Fprintf(os.Stderr, "Please build the CLI first: make build\n")
		os.Exit(1)
	}

	// Create orchestrator
	orchestrator, err := NewOrchestrator(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating orchestrator: %v\n", err)
		os.Exit(1)
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run orchestrator
	log.Printf("[ORCHESTRATOR] Starting orchestrator...")
	startTime := time.Now()

	if err := orchestrator.Run(ctx, cancel); err != nil {
		log.Printf("[ORCHESTRATOR] Orchestrator error: %v", err)
		os.Exit(1)
	}

	duration := time.Since(startTime)
	log.Printf("[ORCHESTRATOR] Orchestrator completed successfully in %v", duration)
}

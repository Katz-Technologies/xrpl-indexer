package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/labstack/echo/v4"
	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/ipc"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/producers"
	"github.com/xrpscan/platform/routes"
	"github.com/xrpscan/platform/signals"
	"github.com/xrpscan/platform/socketio"
)

var (
	// Global IPC protocol for subprocess mode
	ipcProtocol *ipc.ScannerProtocol
)

func main() {
	// Parse command line flags
	mode := flag.String("mode", "server", "Mode: 'server' (default) or 'indexer' (subprocess mode)")
	configFile := flag.String("config", ".env", "Environment config file")
	flag.Parse()

	// Set environment variable for indexer mode BEFORE loading config and logger
	if *mode == "indexer" {
		os.Setenv("INDEXER_MODE", "true")
	}

	// Log config file path for debugging - use log.Printf which goes to stderr
	// IMPORTANT: Write directly to stderr BEFORE logger.New() because log package might be redirected
	if *mode == "indexer" {
		wd, _ := os.Getwd()
		configPath := *configFile
		if !filepath.IsAbs(configPath) {
			configPath = filepath.Join(wd, configPath)
		}
		fmt.Fprintf(os.Stderr, "[INDEXER] Working directory: %s\n", wd)
		fmt.Fprintf(os.Stderr, "[INDEXER] Config file path: %s\n", configPath)
		if _, err := os.Stat(configPath); err == nil {
			fmt.Fprintf(os.Stderr, "[INDEXER] Config file exists\n")
		} else {
			fmt.Fprintf(os.Stderr, "[INDEXER] Config file does not exist: %v\n", err)
		}
		os.Stderr.Sync()
	}

	config.EnvLoad(*configFile)
	fmt.Fprintf(os.Stderr, "[INDEXER] Config loaded, initializing logger...\n")
	os.Stderr.Sync()
	logger.New()
	fmt.Fprintf(os.Stderr, "[INDEXER] Logger initialized, entering runIndexerMode...\n")
	os.Stderr.Sync()

	// If running in indexer mode, initialize IPC and skip HTTP server
	if *mode == "indexer" {
		runIndexerMode()
		return
	}

	// Default server mode
	runServerMode()
}

func runIndexerMode() {
	// Logger is already initialized in main() with INDEXER_MODE set
	// Use logger.Log instead of fmt.Fprintf for consistency
	logger.Log.Info().Msg("========================================")
	logger.Log.Info().Msg("Starting indexer mode...")
	logger.Log.Info().Msg("Indexer mode initialized")

	// Initialize IPC protocol for subprocess communication
	// Note: stdout is used for IPC, stderr is used for logs
	logger.Log.Info().Msg("Initializing IPC protocol...")
	ipcProtocol = ipc.NewScannerProtocol(os.Stdout, os.Stdin)
	producers.InitIPC()
	logger.Log.Info().Msg("IPC protocol initialized")

	// Send ready event to orchestrator
	logger.Log.Info().Msg("Sending ready event to orchestrator...")
	readyEvent := &ipc.Event{
		Type: ipc.EventTypeReady,
		Data: map[string]interface{}{
			"status": "ready",
		},
	}
	if err := ipcProtocol.Send(readyEvent); err != nil {
		logger.Log.Error().Err(err).Msg("Failed to send ready event")
		os.Exit(1)
	}
	logger.Log.Info().Msg("Ready event sent successfully to orchestrator")

	// Initialize connections (but not ClickHouse - orchestrator handles that)
	logger.Log.Info().Msg("Initializing XRPL connections...")
	connections.NewXrplClient()
	connections.NewXrplRPCClient()
	logger.Log.Info().Msg("XRPL connections initialized")

	// Start producers (they will send transactions via IPC)
	logger.Log.Info().Msg("Starting producers and stream subscriptions...")
	go connections.SubscribeStreams()
	go connections.MonitorXRPLConnection()
	go producers.RunProducers()
	logger.Log.Info().Msg("All services started, waiting for ledgers...")

	// In indexer mode, we don't want to exit on signal - orchestrator will handle shutdown
	// Just wait indefinitely (or until context is cancelled)
	// Don't call signals.HandleAll() as it will call os.Exit()
	select {} // Block forever - orchestrator will kill the process when needed
}

func runServerMode() {
	connections.NewClickHouseConnection()
	connections.NewXrplClient()
	connections.NewXrplRPCClient()

	// Initialize SocketIO hub
	socketio.GetHub()

	go connections.SubscribeStreams()
	go connections.MonitorXRPLConnection()
	go producers.RunProducers()
	// Consumers are no longer needed - transactions are processed directly in producers

	e := echo.New()
	e.HideBanner = true
	routes.Add(e)

	signals.HandleAll()
	serverAddress := fmt.Sprintf("%s:%s", config.EnvServerHost(), config.EnvServerPort())
	e.Logger.Fatal(e.Start(serverAddress))
}

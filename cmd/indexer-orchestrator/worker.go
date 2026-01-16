package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/xrpscan/platform/ipc"
)

type IndexerWorker struct {
	ID         int
	subprocess *ipc.Subprocess
	status     WorkerStatus
	startTime  time.Time
	stopTime   *time.Time
	logFile    *os.File
	configFile string
	serverPath string
	serverURL  string
	mu         sync.Mutex
}

type WorkerStatus string

const (
	WorkerStatusRunning   WorkerStatus = "running"
	WorkerStatusStopped   WorkerStatus = "stopped"
	WorkerStatusCompleted WorkerStatus = "completed"
	WorkerStatusFailed    WorkerStatus = "failed"
)

func NewIndexerWorker(id int, serverPath, configFile, serverURL string) (*IndexerWorker, error) {
	// Create log file for this worker
	logPath := fmt.Sprintf("logs/realtime-indexer-worker-%d.log", id)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	return &IndexerWorker{
		ID:         id,
		subprocess: nil, // Will be created in Start()
		status:     WorkerStatusStopped,
		logFile:    logFile,
		configFile: configFile,
		serverPath: serverPath,
		serverURL:  serverURL,
	}, nil
}

func (w *IndexerWorker) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.status == WorkerStatusRunning {
		return fmt.Errorf("worker %d is already running", w.ID)
	}

	// If subprocess exists and is not nil, we need to create a new one
	if w.subprocess != nil {
		// Cleanup old subprocess
		w.subprocess.Close()
	}

	// Get current working directory to set for subprocess
	wd, err := os.Getwd()
	if err != nil {
		w.status = WorkerStatusFailed
		return fmt.Errorf("failed to get working directory for worker %d: %w", w.ID, err)
	}

	// Convert config file path to absolute path if relative
	configPath := w.configFile
	if !filepath.IsAbs(configPath) {
		configPath = filepath.Join(wd, configPath)
	}

	// Verify config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Printf("[WORKER-%d] Warning: Config file does not exist at %s, will try to load anyway", w.ID, configPath)
	} else if err != nil {
		log.Printf("[WORKER-%d] Warning: Error checking config file %s: %v", w.ID, configPath, err)
	} else {
		log.Printf("[WORKER-%d] Config file found at: %s", w.ID, configPath)
	}

	// Verify log file is valid
	if w.logFile == nil {
		log.Printf("[WORKER-%d] ERROR: logFile is nil!", w.ID)
		w.status = WorkerStatusFailed
		return fmt.Errorf("logFile is nil for worker %d", w.ID)
	}

	// Test write to log file
	testMsg := fmt.Sprintf("[WORKER-%d] Test write to log file at %s\n", w.ID, time.Now().Format(time.RFC3339))
	if _, err := w.logFile.WriteString(testMsg); err != nil {
		log.Printf("[WORKER-%d] ERROR: Cannot write to log file: %v", w.ID, err)
		w.status = WorkerStatusFailed
		return fmt.Errorf("cannot write to log file for worker %d: %w", w.ID, err)
	}
	w.logFile.Sync()
	log.Printf("[WORKER-%d] Log file is writable, test message written", w.ID)

	// Create new subprocess with working directory set
	// Pass logFile directly to stderr (like in backfill orchestrator)
	// Set XRPL_WEBSOCKET_URL environment variable for this worker
	envVars := map[string]string{
		"XRPL_WEBSOCKET_URL": w.serverURL,
	}
	log.Printf("[WORKER-%d] Creating subprocess: %s --mode indexer --config %s (server: %s)", w.ID, w.serverPath, configPath, w.serverURL)
	subprocess, err := ipc.NewSubprocessWithDir(w.serverPath, wd, w.logFile, envVars, "--mode", "indexer", "--config", configPath)
	if err != nil {
		log.Printf("[WORKER-%d] ERROR: Failed to create subprocess: %v", w.ID, err)
		w.status = WorkerStatusFailed
		return fmt.Errorf("failed to create subprocess for worker %d: %w", w.ID, err)
	}
	log.Printf("[WORKER-%d] Subprocess created successfully with XRPL_WEBSOCKET_URL=%s", w.ID, w.serverURL)

	w.subprocess = subprocess

	log.Printf("[WORKER-%d] Starting indexer process", w.ID)

	if err := w.subprocess.Start(); err != nil {
		log.Printf("[WORKER-%d] ERROR: Failed to start subprocess: %v", w.ID, err)
		w.status = WorkerStatusFailed
		return fmt.Errorf("failed to start worker %d: %w", w.ID, err)
	}

	w.status = WorkerStatusRunning
	w.startTime = time.Now()

	log.Printf("[WORKER-%d] Started with PID %d, stderr should be writing to log file", w.ID, w.PID())

	// Write another test message after process start
	testMsg2 := fmt.Sprintf("[WORKER-%d] Process started, PID: %d, time: %s\n", w.ID, w.PID(), time.Now().Format(time.RFC3339))
	w.logFile.WriteString(testMsg2)
	w.logFile.Sync()

	return nil
}

func (w *IndexerWorker) Stop(graceful bool) error {
	if w.status != WorkerStatusRunning {
		return nil // Already stopped
	}

	log.Printf("[WORKER-%d] Stopping worker (graceful=%v)...", w.ID, graceful)

	if graceful {
		// Send SIGTERM for graceful shutdown
		if err := w.subprocess.Kill(); err != nil {
			log.Printf("[WORKER-%d] Error sending SIGTERM: %v", w.ID, err)
		}

		// Wait for graceful shutdown with timeout
		done := make(chan error, 1)
		go func() {
			done <- w.subprocess.Wait()
		}()

		select {
		case <-done:
			log.Printf("[WORKER-%d] Gracefully stopped", w.ID)
		case <-time.After(30 * time.Second):
			log.Printf("[WORKER-%d] Graceful shutdown timeout, forcing kill", w.ID)
			w.subprocess.Kill()
			<-done
		}
	} else {
		// Force kill
		w.subprocess.Kill()
		w.subprocess.Wait()
	}

	now := time.Now()
	w.stopTime = &now
	w.status = WorkerStatusStopped

	if w.logFile != nil {
		w.logFile.Close()
	}

	log.Printf("[WORKER-%d] Stopped", w.ID)
	return nil
}

func (w *IndexerWorker) CheckStatus() error {
	if w.status != WorkerStatusRunning {
		return nil
	}

	// Check if process is still running
	processState := w.subprocess.ProcessState()
	if processState != nil {
		// Process has exited
		if processState.Success() {
			w.status = WorkerStatusCompleted
		} else {
			w.status = WorkerStatusFailed
		}
		now := time.Now()
		w.stopTime = &now
		log.Printf("[WORKER-%d] Process exited with status: %v (exit code: %v)", w.ID, w.status, processState.ExitCode())
		return nil
	}

	// On Windows, we need a different approach to check if process is running
	// Check if process state is available (means process has exited)
	// If not available, assume process is still running
	// Don't call Wait() here as it will block until process exits

	// Just check if ProcessState is available - if it is, process has exited
	// If not, process is still running (we'll check again next time)
	return nil
}

func (w *IndexerWorker) updateStatusFromWait() {
	processState := w.subprocess.ProcessState()
	if processState != nil {
		if processState.Success() {
			w.status = WorkerStatusCompleted
		} else {
			w.status = WorkerStatusFailed
		}
		now := time.Now()
		w.stopTime = &now
		log.Printf("[WORKER-%d] Process exited with status: %v (exit code: %v)", w.ID, w.status, processState.ExitCode())
		return
	}

	// Process has exited but we haven't called Wait yet
	// Use a longer timeout for the first check after process might have just started
	done := make(chan error, 1)
	go func() {
		done <- w.subprocess.Wait()
	}()

	// Use longer timeout - process might need time to initialize
	select {
	case err := <-done:
		if err == nil {
			w.status = WorkerStatusCompleted
		} else {
			w.status = WorkerStatusFailed
			// Log the actual error
			log.Printf("[WORKER-%d] Process exited with error: %v", w.ID, err)
		}
		now := time.Now()
		w.stopTime = &now
		log.Printf("[WORKER-%d] Process exited with status: %v", w.ID, w.status)
	case <-time.After(5 * time.Second):
		// Process is still running after 5 seconds - this is good
		// Don't mark as failed, just return
		log.Printf("[WORKER-%d] Process is still running (Wait() didn't return after 5s)", w.ID)
		return
	}
}

func (w *IndexerWorker) IsRunning() bool {
	return w.status == WorkerStatusRunning
}

func (w *IndexerWorker) IsCompleted() bool {
	return w.status == WorkerStatusCompleted
}

func (w *IndexerWorker) IsFailed() bool {
	return w.status == WorkerStatusFailed
}

func (w *IndexerWorker) PID() int {
	return w.subprocess.PID()
}

func (w *IndexerWorker) Stdin() io.WriteCloser {
	return w.subprocess.Stdin()
}

func (w *IndexerWorker) Stdout() io.ReadCloser {
	return w.subprocess.Stdout()
}

func (w *IndexerWorker) Stderr() io.ReadCloser {
	return w.subprocess.Stderr()
}

func (w *IndexerWorker) Cleanup() {
	if w.logFile != nil {
		w.logFile.Close()
	}
	if w.subprocess != nil {
		w.subprocess.Close()
	}
}

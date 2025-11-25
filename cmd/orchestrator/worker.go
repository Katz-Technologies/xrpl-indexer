package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Worker struct {
	ID         int
	PID        int
	Cmd        *exec.Cmd
	FromLedger int
	ToLedger   int
	Server     string
	Status     WorkerStatus
	StartTime  time.Time
	StopTime   *time.Time
	LogFile    *os.File
}

type WorkerStatus string

const (
	WorkerStatusRunning   WorkerStatus = "running"
	WorkerStatusStopped   WorkerStatus = "stopped"
	WorkerStatusCompleted WorkerStatus = "completed"
	WorkerStatusFailed    WorkerStatus = "failed"
)

func NewWorker(id int, fromLedger, toLedger int, ledgers []int, server, cliPath, configFile string, verbose bool, minDelay int64) (*Worker, error) {
	// Check if there's a worker-specific delay override via environment variable
	// Format: BACKFILL_MIN_DELAY_MS_WORKER_1, BACKFILL_MIN_DELAY_MS_WORKER_2, etc.
	workerDelayEnv := fmt.Sprintf("BACKFILL_MIN_DELAY_MS_WORKER_%d", id)
	if envDelay := os.Getenv(workerDelayEnv); envDelay != "" {
		if delay, err := strconv.ParseInt(envDelay, 10, 64); err == nil && delay > 0 {
			log.Printf("[WORKER-%d] Using worker-specific delay from %s: %dms (default was %dms)",
				id, workerDelayEnv, delay, minDelay)
			minDelay = delay
		}
	}
	// Create log file for this worker
	logPath := fmt.Sprintf("logs/orchestrator-worker-%d.log", id)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	// Build command
	args := []string{
		"backfill",
		"-server", server,
		"-config", configFile,
		"-delay", fmt.Sprintf("%d", minDelay),
	}

	// If ledgers list is provided, use it; otherwise use from/to range
	if len(ledgers) > 0 {
		// For large lists (>1000), use file; otherwise use command line argument
		if len(ledgers) > 1000 {
			// Save to file
			ledgersFile := fmt.Sprintf("logs/orchestrator-worker-%d-ledgers.txt", id)
			file, err := os.Create(ledgersFile)
			if err != nil {
				return nil, fmt.Errorf("failed to create ledgers file: %w", err)
			}
			for _, ledger := range ledgers {
				fmt.Fprintf(file, "%d\n", ledger)
			}
			file.Close()
			args = append(args, "-ledgers-file", ledgersFile)
			log.Printf("[WORKER-%d] Using ledgers file with %d ledgers: %s", id, len(ledgers), ledgersFile)
		} else {
			// Use command line argument
			ledgersStr := ""
			for i, ledger := range ledgers {
				if i > 0 {
					ledgersStr += ","
				}
				ledgersStr += fmt.Sprintf("%d", ledger)
			}
			args = append(args, "-ledgers", ledgersStr)
			log.Printf("[WORKER-%d] Using ledgers list with %d ledgers", id, len(ledgers))
		}
	} else {
		// Fallback to from/to range
		args = append(args, "-from", fmt.Sprintf("%d", fromLedger))
		args = append(args, "-to", fmt.Sprintf("%d", toLedger))
	}

	if verbose {
		args = append(args, "-verbose")
	}

	cmd := exec.Command(cliPath, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = os.Environ()
	// Set log file path for the worker
	cmd.Env = append(cmd.Env, fmt.Sprintf("LOG_FILE_PATH=%s", logPath))

	worker := &Worker{
		ID:         id,
		FromLedger: fromLedger,
		ToLedger:   toLedger,
		Server:     server,
		Status:     WorkerStatusStopped,
		LogFile:    logFile,
		Cmd:        cmd,
	}

	return worker, nil
}

func (w *Worker) Start() error {
	if w.Status == WorkerStatusRunning {
		return fmt.Errorf("worker %d is already running", w.ID)
	}

	log.Printf("[WORKER-%d] Starting backfill from ledger %d to %d on server %s",
		w.ID, w.FromLedger, w.ToLedger, w.Server)

	err := w.Cmd.Start()
	if err != nil {
		w.Status = WorkerStatusFailed
		return fmt.Errorf("failed to start worker %d: %w", w.ID, err)
	}

	w.PID = w.Cmd.Process.Pid
	w.Status = WorkerStatusRunning
	w.StartTime = time.Now()

	log.Printf("[WORKER-%d] Started with PID %d", w.ID, w.PID)
	return nil
}

func (w *Worker) Stop(graceful bool) error {
	if w.Status != WorkerStatusRunning {
		return nil // Already stopped
	}

	log.Printf("[WORKER-%d] Stopping worker (graceful=%v)...", w.ID, graceful)

	if w.Cmd.Process == nil {
		w.Status = WorkerStatusStopped
		return nil
	}

	if graceful {
		// On Windows, SIGTERM is not supported
		// For graceful shutdown on Windows, we just wait for the process to finish naturally
		// On Unix systems, we use SIGTERM for graceful shutdown
		if runtime.GOOS != "windows" {
			// On Unix systems, use SIGTERM for graceful shutdown
			err := w.Cmd.Process.Signal(syscall.SIGTERM)
			if err != nil {
				log.Printf("[WORKER-%d] Error sending SIGTERM: %v", w.ID, err)
			} else {
				log.Printf("[WORKER-%d] Sent SIGTERM for graceful shutdown", w.ID)
			}
		} else {
			// On Windows, we can't send signals to child processes reliably
			// Just wait for the process to finish naturally (it should handle shutdown on its own)
			log.Printf("[WORKER-%d] Waiting for process to finish naturally (Windows graceful shutdown)", w.ID)
		}

		// Wait for graceful shutdown with timeout
		done := make(chan error, 1)
		go func() {
			done <- w.Cmd.Wait()
		}()

		select {
		case <-done:
			log.Printf("[WORKER-%d] Gracefully stopped", w.ID)
		case <-time.After(30 * time.Second):
			log.Printf("[WORKER-%d] Graceful shutdown timeout, forcing kill", w.ID)
			err := w.Cmd.Process.Kill()
			if err != nil {
				log.Printf("[WORKER-%d] Error killing process: %v", w.ID, err)
			}
			<-done // Wait for kill to complete
		}
	} else {
		// Force kill
		// Check if process has already finished
		if w.Cmd.ProcessState != nil {
			// Process has already finished, no need to kill
			log.Printf("[WORKER-%d] Process already finished, skipping kill", w.ID)
		} else if w.Cmd.Process != nil {
			// Check if process is still running by trying to send signal 0
			process, err := os.FindProcess(w.PID)
			if err == nil {
				err = process.Signal(syscall.Signal(0))
				if err == nil {
					// Process is still running, kill it
					err = w.Cmd.Process.Kill()
					if err != nil {
						// Process might have finished between check and kill - this is not an error
						if !strings.Contains(err.Error(), "already finished") {
							log.Printf("[WORKER-%d] Error killing process: %v", w.ID, err)
						}
					}
				} else {
					// Process has already finished
					log.Printf("[WORKER-%d] Process already finished, skipping kill", w.ID)
				}
			}
		}
		w.Cmd.Wait() // Wait for process to exit (safe even if already finished)
	}

	now := time.Now()
	w.StopTime = &now
	w.Status = WorkerStatusStopped

	if w.LogFile != nil {
		w.LogFile.Close()
	}

	log.Printf("[WORKER-%d] Stopped", w.ID)
	return nil
}

func (w *Worker) CheckStatus() error {
	if w.Status != WorkerStatusRunning {
		return nil
	}

	// Don't check status too early after start - give the process time to initialize
	// This prevents false positives on Windows where Signal(0) might fail for newly started processes
	if time.Since(w.StartTime) < 3*time.Second {
		// Process just started, don't check status yet
		return nil
	}

	// First, try to get process state if available (process might have exited)
	if w.Cmd.ProcessState != nil {
		// Process has exited
		// Only update and log if status is changing (not already set)
		if w.Status == WorkerStatusRunning {
			if w.Cmd.ProcessState.Success() {
				w.Status = WorkerStatusCompleted
			} else {
				w.Status = WorkerStatusFailed
			}
			now := time.Now()
			w.StopTime = &now
			log.Printf("[WORKER-%d] Process exited with status: %v", w.ID, w.Status)
		}
		return nil
	}

	// First, check if process is a zombie (defunct) - this is the most reliable check
	// On Linux, we can check /proc/PID/stat to see if process is zombie
	// Check this BEFORE Signal(0) because Signal(0) may succeed on zombie processes
	// Note: This is Linux-specific, on Windows we skip this check
	if runtime.GOOS == "linux" {
		statPath := fmt.Sprintf("/proc/%d/stat", w.PID)
		if statData, err := os.ReadFile(statPath); err == nil {
			// Parse stat file - third field is state
			// 'Z' means zombie
			fields := strings.Fields(string(statData))
			if len(fields) > 2 && fields[2] == "Z" {
				// Process is zombie, it has exited
				log.Printf("[WORKER-%d] Process is zombie (defunct), waiting for it", w.ID)
				w.updateStatusFromWait()
				return nil
			}
		}
	}

	// On Windows, Signal(0) is unreliable, so we use a different approach
	if runtime.GOOS == "windows" {
		// On Windows, check if ProcessState is available (set automatically when process exits)
		// If ProcessState is nil, process is still running
		if w.Cmd.ProcessState != nil {
			// Process has exited - ProcessState is set automatically
			// Only update and log if status is changing (not already set)
			if w.Status == WorkerStatusRunning {
				if w.Cmd.ProcessState.Success() {
					w.Status = WorkerStatusCompleted
				} else {
					w.Status = WorkerStatusFailed
				}
				now := time.Now()
				w.StopTime = &now
				log.Printf("[WORKER-%d] Process exited with status: %v", w.ID, w.Status)
			}
			return nil
		}

		// ProcessState is nil, but process might have exited
		// On Windows, we need to explicitly call Wait() to get ProcessState
		// But Wait() blocks if process is still running, so we use a goroutine with timeout
		done := make(chan struct {
			err error
			ok  bool
		}, 1)
		go func() {
			err := w.Cmd.Wait()
			done <- struct {
				err error
				ok  bool
			}{err: err, ok: true}
		}()

		select {
		case result := <-done:
			// Process has exited (Wait() returned)
			// Only update and log if status is changing (not already set)
			if w.Status == WorkerStatusRunning {
				if result.err == nil {
					w.Status = WorkerStatusCompleted
				} else {
					w.Status = WorkerStatusFailed
				}
				now := time.Now()
				w.StopTime = &now
				log.Printf("[WORKER-%d] Process exited with status: %v", w.ID, w.Status)
			}
			return nil
		case <-time.After(200 * time.Millisecond):
			// Wait() is still blocking, process is still running
			// Note: We can't cancel the Wait() goroutine, but that's okay
			// It will complete when the process exits and set ProcessState
			return nil
		}
	}

	// On Unix systems, use Signal(0) which is reliable
	process, err := os.FindProcess(w.PID)
	if err != nil {
		// Process not found - but wait a bit more if process just started
		if time.Since(w.StartTime) < 5*time.Second {
			// Process just started, might not be fully registered yet
			return nil
		}
		// Process not found and enough time has passed, it has exited
		w.updateStatusFromWait()
		return nil
	}

	// Try to send signal 0 to check if process exists
	err = process.Signal(syscall.Signal(0))
	if err != nil {
		// Signal failed - but wait a bit more if process just started
		if time.Since(w.StartTime) < 5*time.Second {
			// Process just started, might not be ready yet
			return nil
		}
		// Process is not running (likely exited) - try to wait for it
		w.updateStatusFromWait()
		return nil
	}

	// Process is still running
	return nil
}

func (w *Worker) updateStatusFromWait() {
	// Check if we already have process state
	if w.Cmd.ProcessState != nil {
		if w.Cmd.ProcessState.Success() {
			w.Status = WorkerStatusCompleted
		} else {
			w.Status = WorkerStatusFailed
		}
		now := time.Now()
		w.StopTime = &now
		// Only log if status is changing (not already set)
		if w.Status == WorkerStatusRunning {
			log.Printf("[WORKER-%d] Process exited with status: %v", w.ID, w.Status)
		}
		return
	}

	// Process has exited but we haven't called Wait yet
	// Use a channel with timeout to avoid blocking
	// For completed processes, Wait() should return immediately
	done := make(chan error, 1)
	go func() {
		done <- w.Cmd.Wait()
	}()

	select {
	case err := <-done:
		// Wait() returned - process has exited
		// ProcessState should now be set
		if w.Cmd.ProcessState != nil {
			if w.Cmd.ProcessState.Success() {
				w.Status = WorkerStatusCompleted
			} else {
				w.Status = WorkerStatusFailed
			}
		} else {
			// ProcessState not set yet, use error to determine status
			if err == nil {
				w.Status = WorkerStatusCompleted
			} else {
				w.Status = WorkerStatusFailed
			}
		}
		now := time.Now()
		w.StopTime = &now
		// Only log if status is changing (not already set)
		if w.Status == WorkerStatusRunning {
			log.Printf("[WORKER-%d] Process exited with status: %v", w.ID, w.Status)
		}
	case <-time.After(100 * time.Millisecond):
		// Wait() didn't return quickly - process is still running
		// Don't update status, just return
		// The goroutine will continue waiting and set ProcessState when process exits
		return
	}
}

func (w *Worker) GetRange() (int, int) {
	return w.FromLedger, w.ToLedger
}

func (w *Worker) IsRunning() bool {
	return w.Status == WorkerStatusRunning
}

func (w *Worker) IsCompleted() bool {
	return w.Status == WorkerStatusCompleted
}

func (w *Worker) IsFailed() bool {
	return w.Status == WorkerStatusFailed
}

func (w *Worker) Cleanup() {
	if w.LogFile != nil {
		w.LogFile.Close()
	}
}

// SetupSignalHandlers sets up signal handlers for graceful shutdown
// Note: This function is kept for compatibility but signal handling is now done in orchestrator.Run()
func SetupSignalHandlers(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v, shutting down gracefully...", sig)
		cancel()
	}()
}

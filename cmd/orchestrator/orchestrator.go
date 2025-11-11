package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/logger"
)

type Orchestrator struct {
	config          *Config
	workers         []*Worker
	progressTracker *ProgressTracker
	currentFrom     int
	currentTo       int
	gapsFilled      bool // Flag to ensure gaps are filled only once per cycle
	chInitialized   bool // Flag to track if ClickHouse connection is initialized
}

func NewOrchestrator(cfg *Config) (*Orchestrator, error) {
	// Ensure logs directory exists
	if err := os.MkdirAll("logs", 0755); err != nil {
		return nil, fmt.Errorf("failed to create logs directory: %w", err)
	}

	// Verify CLI path exists
	if _, err := os.Stat(cfg.CLIPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("CLI executable not found at %s", cfg.CLIPath)
	}

	progressTracker := NewProgressTracker(cfg.FromLedger, cfg.ToLedger)

	return &Orchestrator{
		config:          cfg,
		workers:         make([]*Worker, 0, cfg.Workers),
		progressTracker: progressTracker,
		currentFrom:     cfg.FromLedger,
		currentTo:       cfg.ToLedger,
		gapsFilled:      false,
	}, nil
}

func (o *Orchestrator) Run(ctx context.Context, cancel context.CancelFunc) error {
	log.Printf("[ORCHESTRATOR] Starting orchestrator with %d workers", o.config.Workers)
	log.Printf("[ORCHESTRATOR] Target range: %d-%d", o.config.FromLedger, o.config.ToLedger)
	log.Printf("[ORCHESTRATOR] Servers: %v", o.config.Servers)
	log.Printf("[ORCHESTRATOR] Check interval: %v", o.config.CheckInterval)

	// Initialize ClickHouse connection once for the entire orchestrator
	if !o.chInitialized {
		config.EnvLoad(o.config.ConfigFile)
		logger.New()
		connections.NewClickHouseConnection()
		o.chInitialized = true
		log.Printf("[ORCHESTRATOR] ClickHouse connection initialized")
	}

	// Setup signal handlers for graceful shutdown
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-stopChan
		log.Printf("[ORCHESTRATOR] Received signal: %v, shutting down gracefully...", sig)
		o.StopAllWorkers(true)
		// Close ClickHouse connection on shutdown
		if o.chInitialized {
			connections.CloseClickHouse()
		}
		cancel() // Cancel context to exit main loop
	}()

	// Close ClickHouse connection on exit
	defer func() {
		if o.chInitialized {
			connections.CloseClickHouse()
		}
	}()

	// Check for stop file periodically
	stopFileCheckTicker := time.NewTicker(5 * time.Second)
	defer stopFileCheckTicker.Stop()

	// Main orchestration loop
	for {
		select {
		case <-ctx.Done():
			log.Printf("[ORCHESTRATOR] Context cancelled, shutting down...")
			o.StopAllWorkers(true)
			return nil
		case <-stopFileCheckTicker.C:
			// Check for stop file
			if _, err := os.Stat("stop.orchestrator"); err == nil {
				log.Printf("[ORCHESTRATOR] Stop file detected, shutting down gracefully...")
				o.StopAllWorkers(true)
				// Remove stop file
				os.Remove("stop.orchestrator")
				return nil
			}
		default:
			// Check if we're done
			if o.currentFrom > o.currentTo {
				log.Printf("[ORCHESTRATOR] All ledgers processed, exiting")
				return nil
			}

			// Start workers if not running
			if len(o.workers) == 0 || !o.hasRunningWorkers() {
				// Check if we're done before trying to start workers
				if o.currentFrom > o.currentTo {
					log.Printf("[ORCHESTRATOR] All ledgers processed, exiting")
					return nil
				}

				if err := o.startWorkers(); err != nil {
					return fmt.Errorf("failed to start workers: %w", err)
				}

				// Check again after startWorkers - it might have determined we're done
				if o.currentFrom > o.currentTo {
					log.Printf("[ORCHESTRATOR] All ledgers processed (determined by startWorkers), exiting")
					return nil
				}

				// If no workers were started (all work is done), exit
				if len(o.workers) == 0 {
					log.Printf("[ORCHESTRATOR] No workers started - all work is complete, exiting")
					return nil
				}

				o.gapsFilled = false // Reset gaps filled flag for new cycle
			}

			// Check worker status
			completedWorker := o.checkWorkers()

			// Check if all workers have completed
			allCompleted := true
			anyRunning := false
			for _, worker := range o.workers {
				if worker.IsRunning() {
					anyRunning = true
					allCompleted = false
					break
				}
				if !worker.IsCompleted() && !worker.IsFailed() {
					allCompleted = false
				}
			}

			// If all workers completed and none are running, redistribute work
			if allCompleted && !anyRunning && len(o.workers) > 0 {
				log.Printf("[ORCHESTRATOR] All workers completed, redistributing work")
				completedWorker = o.workers[0] // Use first worker as trigger
			}

			if completedWorker != nil {
				log.Printf("[ORCHESTRATOR] ========================================")
				log.Printf("[ORCHESTRATOR] Worker %d completed", completedWorker.ID)
				log.Printf("[ORCHESTRATOR] ========================================")

				// Check if we should let other workers finish their work
				shouldStopAll := true
				if o.config.RedistributeThreshold > 0 {
					// Check remaining work for other running workers
					maxRemaining := 0
					for _, worker := range o.workers {
						if worker.IsRunning() && worker.ID != completedWorker.ID {
							from, to := worker.GetRange()
							remaining, err := CountRemainingLedgers(from, to)
							if err != nil {
								log.Printf("[ORCHESTRATOR] Error checking remaining ledgers for worker %d: %v", worker.ID, err)
								// On error, default to stopping
								continue
							}
							if remaining > maxRemaining {
								maxRemaining = remaining
							}
							log.Printf("[ORCHESTRATOR] Worker %d has %d ledgers remaining (range %d-%d)",
								worker.ID, remaining, from, to)
						}
					}

					if maxRemaining > 0 && maxRemaining <= o.config.RedistributeThreshold {
						log.Printf("[ORCHESTRATOR] Other workers have %d or fewer ledgers remaining (threshold: %d), letting them finish",
							maxRemaining, o.config.RedistributeThreshold)
						shouldStopAll = false
					} else if maxRemaining > 0 {
						log.Printf("[ORCHESTRATOR] Other workers have %d ledgers remaining (threshold: %d), stopping and redistributing",
							maxRemaining, o.config.RedistributeThreshold)
					}
				}

				if shouldStopAll {
					// Stop all workers FIRST - this is critical to prevent duplicate processing
					log.Printf("[ORCHESTRATOR] Stopping all workers before redistribution...")
					o.StopAllWorkers(true)
					log.Printf("[ORCHESTRATOR] All workers stopped, now proceeding with gap filling and redistribution")
				} else {
					// Wait for other workers to finish
					log.Printf("[ORCHESTRATOR] Waiting for other workers to finish their remaining work...")
					// Continue checking until all workers complete
					for {
						allDone := true
						for _, worker := range o.workers {
							if worker.IsRunning() {
								allDone = false
								break
							}
						}
						if allDone {
							log.Printf("[ORCHESTRATOR] All workers have finished")
							break
						}
						// Check status of all workers
						o.checkWorkers()
						time.Sleep(5 * time.Second) // Check every 5 seconds
					}
					// Now proceed with redistribution
					log.Printf("[ORCHESTRATOR] All workers finished, proceeding with gap filling and redistribution")
				}

				// Fill gaps once per cycle (if not already filled)
				// Fill gaps in the FULL original range to catch all gaps
				if !o.gapsFilled {
					log.Printf("[ORCHESTRATOR] Attempting to fill gaps in full range %d-%d (one-time per cycle)",
						o.config.FromLedger, o.config.ToLedger)
					// Use full original range for gap filling
					o.progressTracker = NewProgressTracker(o.config.FromLedger, o.config.ToLedger)
					if err := o.fillGaps(); err != nil {
						log.Printf("[ORCHESTRATOR] Error filling gaps: %v (continuing anyway)", err)
					}
					o.gapsFilled = true
					log.Printf("[ORCHESTRATOR] Gap filling completed")
				} else {
					log.Printf("[ORCHESTRATOR] Gaps already filled in this cycle, skipping gap filling")
				}

				// Calculate remaining range and redistribute AFTER gap filling
				// This ensures new workers get ranges that don't overlap with gap-filling work
				log.Printf("[ORCHESTRATOR] Calculating remaining work after gap filling...")
				o.progressTracker = NewProgressTracker(o.config.FromLedger, o.config.ToLedger)
				if err := o.redistributeWork(); err != nil {
					return fmt.Errorf("failed to redistribute work: %w", err)
				}

				// Check if there's any remaining work
				if o.currentFrom > o.currentTo {
					log.Printf("[ORCHESTRATOR] No remaining work after redistribution, exiting")
					return nil
				}

				// Clear workers for next iteration
				log.Printf("[ORCHESTRATOR] Preparing to restart workers with new ranges...")
				o.workers = []*Worker{}
				o.gapsFilled = false // Reset gaps filled flag for new cycle

				// Immediately start new workers with redistributed work
				// Don't wait for next check interval
				log.Printf("[ORCHESTRATOR] Immediately starting new workers with redistributed ranges...")
				if err := o.startWorkers(); err != nil {
					return fmt.Errorf("failed to start workers after redistribution: %w", err)
				}

				// Check if workers were started
				if len(o.workers) == 0 {
					log.Printf("[ORCHESTRATOR] No workers started after redistribution - all work is complete")
					return nil
				}

				log.Printf("[ORCHESTRATOR] Successfully restarted %d workers with new ranges", len(o.workers))
			}

			// Sleep before next check
			time.Sleep(o.config.CheckInterval)
		}
	}
}

func (o *Orchestrator) startWorkers() error {
	log.Printf("[ORCHESTRATOR] Starting %d workers for range %d-%d",
		o.config.Workers, o.currentFrom, o.currentTo)

	// Calculate remaining range
	remainingFrom, remainingTo, err := o.progressTracker.GetRemainingRange()
	if err != nil {
		return fmt.Errorf("failed to get remaining range: %w", err)
	}

	if remainingFrom > remainingTo {
		log.Printf("[ORCHESTRATOR] All ledgers in current range are indexed")
		o.currentFrom = o.currentTo + 1
		return nil
	}

	// Update current range
	o.currentFrom = remainingFrom
	o.currentTo = remainingTo

	// Split range into N parts
	ranges := SplitRange(remainingFrom, remainingTo, o.config.Workers)
	log.Printf("[ORCHESTRATOR] Split range into %d parts: %v", len(ranges), ranges)

	// Create and start workers
	o.workers = make([]*Worker, 0, len(ranges))
	for i, r := range ranges {
		// Distribute servers using round-robin
		server := o.config.Servers[i%len(o.config.Servers)]

		worker, err := NewWorker(
			i+1,
			r.From,
			r.To,
			server,
			o.config.CLIPath,
			o.config.ConfigFile,
			o.config.Verbose,
			o.config.MinDelay,
		)
		if err != nil {
			// Cleanup already created workers
			o.StopAllWorkers(false)
			return fmt.Errorf("failed to create worker %d: %w", i+1, err)
		}

		if err := worker.Start(); err != nil {
			// Cleanup already created workers
			o.StopAllWorkers(false)
			return fmt.Errorf("failed to start worker %d: %w", i+1, err)
		}

		o.workers = append(o.workers, worker)
		log.Printf("[ORCHESTRATOR] Started worker %d: range %d-%d on server %s",
			worker.ID, r.From, r.To, server)
	}

	return nil
}

func (o *Orchestrator) checkWorkers() *Worker {
	for _, worker := range o.workers {
		if err := worker.CheckStatus(); err != nil {
			log.Printf("[ORCHESTRATOR] Error checking worker %d: %v", worker.ID, err)
		}

		if worker.IsCompleted() || worker.IsFailed() {
			return worker
		}
	}
	return nil
}

func (o *Orchestrator) hasRunningWorkers() bool {
	for _, worker := range o.workers {
		if worker.IsRunning() {
			return true
		}
	}
	return false
}

func (o *Orchestrator) StopAllWorkers(graceful bool) {
	log.Printf("[ORCHESTRATOR] Stopping all workers (graceful=%v)...", graceful)
	for _, worker := range o.workers {
		if err := worker.Stop(graceful); err != nil {
			log.Printf("[ORCHESTRATOR] Error stopping worker %d: %v", worker.ID, err)
		}
	}
	log.Printf("[ORCHESTRATOR] All workers stopped")
}

func (o *Orchestrator) fillGaps() error {
	log.Printf("[ORCHESTRATOR] Starting gap filling phase...")

	// Find gaps in the current range
	gaps, err := o.progressTracker.FindGaps()
	if err != nil {
		return fmt.Errorf("failed to find gaps: %w", err)
	}

	if len(gaps) == 0 {
		log.Printf("[ORCHESTRATOR] No gaps found, skipping gap filling")
		return nil
	}

	log.Printf("[ORCHESTRATOR] Found %d gaps, attempting to fill them", len(gaps))

	// To ensure we always use o.config.Workers workers, we split all gaps
	// into exactly o.config.Workers parts, regardless of how many gap ranges exist
	// This ensures consistent parallelism
	firstGap := gaps[0]
	lastGap := gaps[len(gaps)-1]

	// Split the gap range into o.config.Workers parts
	gapRanges := SplitRange(firstGap, lastGap, o.config.Workers)
	log.Printf("[ORCHESTRATOR] Split %d gaps (range %d-%d) into %d parts for gap filling",
		len(gaps), firstGap, lastGap, len(gapRanges))

	// Process gap ranges in parallel with exactly o.config.Workers workers
	maxConcurrent := o.config.Workers
	log.Printf("[ORCHESTRATOR] Will process %d gap ranges with %d workers",
		len(gapRanges), maxConcurrent)

	// Use a semaphore to limit concurrent gap filling
	semaphore := make(chan struct{}, maxConcurrent)
	results := make(chan error, len(gapRanges))

	for i, gapRange := range gapRanges {
		go func(idx int, gr Range) {
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			log.Printf("[ORCHESTRATOR] Processing gap range %d/%d: %d-%d",
				idx+1, len(gapRanges), gr.From, gr.To)

			// Use round-robin to select server
			server := o.config.Servers[idx%len(o.config.Servers)]

			// Create a temporary worker to fill this gap range
			// Use idx+1 for gap-filling workers (1, 2, 3...)
			// This is safe because regular workers are stopped before gap filling
			worker, err := NewWorker(
				idx+1, // Gap-filling worker ID (1, 2, 3...)
				gr.From,
				gr.To,
				server,
				o.config.CLIPath,
				o.config.ConfigFile,
				o.config.Verbose,
				o.config.MinDelay,
			)
			if err != nil {
				log.Printf("[ORCHESTRATOR] Failed to create gap-filling worker: %v", err)
				results <- err
				return
			}

			// Start the worker
			if err := worker.Start(); err != nil {
				log.Printf("[ORCHESTRATOR] Failed to start gap-filling worker: %v", err)
				worker.Cleanup()
				results <- err
				return
			}

			// Wait for completion with timeout
			done := make(chan error, 1)
			go func() {
				done <- worker.Cmd.Wait()
			}()

			// Wait for completion or timeout (max 10 minutes per gap range)
			timeout := 10 * time.Minute
			select {
			case err := <-done:
				if err != nil {
					log.Printf("[ORCHESTRATOR] Gap range %d-%d failed: %v", gr.From, gr.To, err)
				} else {
					log.Printf("[ORCHESTRATOR] Gap range %d-%d filled successfully", gr.From, gr.To)
				}
				results <- err
			case <-time.After(timeout):
				log.Printf("[ORCHESTRATOR] Gap range %d-%d timed out after %v, stopping",
					gr.From, gr.To, timeout)
				worker.Stop(false)
				results <- fmt.Errorf("timeout")
			}

			// Cleanup
			worker.Stop(false)
			worker.Cleanup()
		}(i, gapRange)
	}

	// Wait for all gap filling operations to complete
	// We don't fail if some gaps fail - they might be intentionally skipped
	successCount := 0
	failCount := 0
	for i := 0; i < len(gapRanges); i++ {
		err := <-results
		if err == nil {
			successCount++
		} else {
			failCount++
		}
	}

	log.Printf("[ORCHESTRATOR] Gap filling phase completed: %d succeeded, %d failed",
		successCount, failCount)
	log.Printf("[ORCHESTRATOR] All gap-filling workers have finished, safe to start new regular workers")
	return nil
}

func (o *Orchestrator) redistributeWork() error {
	log.Printf("[ORCHESTRATOR] Redistributing work...")

	// Update progress tracker with current range
	o.progressTracker = NewProgressTracker(o.currentFrom, o.currentTo)

	// Calculate remaining range
	remainingFrom, remainingTo, err := o.progressTracker.GetRemainingRange()
	if err != nil {
		return fmt.Errorf("failed to get remaining range: %w", err)
	}

	if remainingFrom > remainingTo {
		log.Printf("[ORCHESTRATOR] All ledgers are indexed, no work to redistribute")
		o.currentFrom = o.currentTo + 1
		return nil
	}

	log.Printf("[ORCHESTRATOR] Remaining range: %d-%d", remainingFrom, remainingTo)
	o.currentFrom = remainingFrom
	o.currentTo = remainingTo

	return nil
}

// VerifyCLIExists checks if the CLI executable exists and is executable
func VerifyCLIExists(cliPath string) error {
	info, err := os.Stat(cliPath)
	if os.IsNotExist(err) {
		return fmt.Errorf("CLI executable not found at %s", cliPath)
	}
	if err != nil {
		return fmt.Errorf("failed to stat CLI executable: %w", err)
	}
	if info.Mode().Perm()&0111 == 0 {
		return fmt.Errorf("CLI executable is not executable: %s", cliPath)
	}
	return nil
}

// TestCLIExecution tests if the CLI can be executed
func TestCLIExecution(cliPath string) error {
	cmd := exec.Command(cliPath, "--help")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("CLI executable test failed: %w", err)
	}
	return nil
}

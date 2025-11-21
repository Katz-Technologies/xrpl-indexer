package connections

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/xrpscan/platform/shutdown"
)

// closeWithTimeout executes a close function with a 3-second timeout
// Also respects the global shutdown context for early termination
func closeWithTimeout(name string, closeFn func() error) {
	// Check if we're already shutting down
	if shutdown.IsActive() {
		shutdownCtx := shutdown.GetContext()
		// Create a timeout context that also respects shutdown context
		ctx, cancel := context.WithTimeout(shutdownCtx, 3*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- closeFn()
		}()

		select {
		case err := <-done:
			if err != nil {
				log.Printf("Error closing %s: %v", name, err)
			} else {
				log.Printf("Successfully closed %s", name)
			}
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				log.Printf("Timeout closing %s after 3 seconds", name)
			} else {
				log.Printf("Shutdown cancelled while closing %s", name)
			}
		}
	} else {
		// Fallback to simple timeout if not in shutdown mode
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- closeFn()
		}()

		select {
		case err := <-done:
			if err != nil {
				log.Printf("Error closing %s: %v", name, err)
			} else {
				log.Printf("Successfully closed %s", name)
			}
		case <-ctx.Done():
			log.Printf("Timeout closing %s after 3 seconds", name)
		}
	}
}

func CloseXrplClient() {
	// Flush all pending ClickHouse batches before closing XRPL connection
	if err := FlushClickHouse(); err != nil {
		log.Printf("Error flushing ClickHouse before closing XRPL client: %v", err)
	}

	closeWithTimeout("XRPL client", func() error {
		if XrplClient != nil {
			return XrplClient.Close()
		}
		return nil
	})
}

func CloseXrplRPCClient() {
	closeWithTimeout("XRPL RPC client", func() error {
		if XrplRPCClient != nil {
			return XrplRPCClient.Close()
		}
		return nil
	})
}

func CloseAll() {
	log.Println("Closing all connections")

	// First unsubscribe from streams
	UnsubscribeStreams()

	// Close other connections in parallel
	var wg sync.WaitGroup

	// Close XRPL client
	wg.Add(1)
	go func() {
		defer wg.Done()
		CloseXrplClient()
	}()
	// Close XRPL RPC client
	wg.Add(1)
	go func() {
		defer wg.Done()
		CloseXrplRPCClient()
	}()

	// Close ClickHouse connection
	wg.Add(1)
	go func() {
		defer wg.Done()
		CloseClickHouse()
	}()

	// Wait for all closures with overall timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All connections closed successfully")
	case <-time.After(15 * time.Second):
		log.Println("Timeout waiting for all connections to close")
	}
}

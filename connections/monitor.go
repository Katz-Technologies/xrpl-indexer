package connections

import (
	"fmt"
	"time"

	"github.com/xrpscan/platform/logger"
)

// MonitorXRPLConnection periodically pings the XRPL streaming client.
// If ping fails, it recreates the client and re-subscribes to streams.
func MonitorXRPLConnection() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Check if client exists (with mutex-like check to avoid race condition)
		client := XrplClient
		if client == nil {
			continue
		}

		// Use recover to handle potential panics from Ping() if connection is nil
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic during ping: %v", r)
					logger.Log.Error().
						Interface("panic", r).
						Msg("Panic occurred during XRPL client ping - connection may be nil")
				}
			}()
			err = client.Ping([]byte("ping"))
		}()

		if err != nil {
			logger.Log.Warn().Err(err).Msg("XRPL ping failed; reconnecting and resubscribing")

			// Close old client (ignore errors)
			CloseXrplClient()

			// Recreate client and resubscribe
			NewXrplClient()
			SubscribeStreams()
		}
	}
}

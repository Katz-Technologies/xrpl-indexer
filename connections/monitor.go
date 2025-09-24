package connections

import (
	"time"

	"github.com/xrpscan/platform/logger"
)

// MonitorXRPLConnection periodically pings the XRPL streaming client.
// If ping fails, it recreates the client and re-subscribes to streams.
func MonitorXRPLConnection() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if XrplClient == nil {
			continue
		}

		if err := XrplClient.Ping([]byte("ping")); err != nil {
			logger.Log.Warn().Err(err).Msg("XRPL ping failed; reconnecting and resubscribing")

			// Close old client (ignore errors)
			CloseXrplClient()

			// Recreate client and resubscribe
			NewXrplClient()
			SubscribeStreams()
		}
	}
}

package connections

import (
	"time"
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
			ReconnectXRPL(nil, "monitor client nil")
			continue
		}

		err := safePingXRPL(client, []byte("ping"), "monitor")

		if err != nil {
			ReconnectXRPL(err, "monitor ping failed")
		}
	}
}

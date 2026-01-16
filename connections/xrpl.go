package connections

import (
	"fmt"
	"time"

	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/xrpl-go"
)

var XrplClient *xrpl.Client

func NewXrplClient() {
	NewXrplClientWithURL(config.EnvXrplWebsocketURL())
}

func NewXrplClientWithURL(URL string) {
	// Infinite retry loop with exponential backoff until a successful ping
	backoff := time.Second
	maxBackoff := 30 * time.Second
	attempt := 0

	for {
		attempt++
		logger.Log.Info().Str("url", URL).Int("attempt", attempt).Msg("Attempting to connect XRPL client")

		// Close old client if exists before creating new one
		if XrplClient != nil {
			// Close in background to avoid blocking
			go func() {
				if err := XrplClient.Close(); err != nil {
					logger.Log.Debug().Err(err).Msg("Error closing old XRPL client during reconnection")
				}
			}()
		}

		XrplClient = xrpl.NewClient(xrpl.ClientConfig{URL: URL})

		// Check if client was created successfully
		if XrplClient == nil {
			logger.Log.Warn().Str("url", URL).Int("attempt", attempt).Msg("Failed to create XRPL client (nil returned)")
			time.Sleep(backoff)
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
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
						Str("url", URL).
						Int("attempt", attempt).
						Msg("Panic occurred during XRPL client ping - connection may be nil")
				}
			}()
			err = XrplClient.Ping([]byte(URL))
		}()

		if err == nil {
			logger.Log.Info().Str("url", URL).Int("attempt", attempt).Msg("Successfully connected XRPL client")
			return
		}

		logger.Log.Warn().Str("url", URL).Int("attempt", attempt).Dur("retry_in", backoff).Err(err).Msg("XRPL connect failed; retrying infinitely")
		time.Sleep(backoff)
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

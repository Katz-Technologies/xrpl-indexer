package connections

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/xrpl-go"
)

var XrplClient *xrpl.Client

// nextXRPLStreamURLIndex rotates through URLs on each reconnect (disconnect → try next node)
var nextXRPLStreamURLIndex atomic.Uint32

func NewXrplClient() {
	NewXrplClientWithURL(config.EnvXrplWebsocketURL())
}

func NewXrplClientWithURL(URL string) {
	// Infinite retry loop with exponential backoff until a successful ping
	backoff := time.Second
	maxBackoff := 30 * time.Second
	attempt := 0
	urls := parseXRPLURLs(URL)
	if len(urls) == 0 {
		urls = []string{URL}
	}

	for {
		attempt++
		// On each connection attempt, rotate to next URL (disconnect → try next node)
		idx := int(nextXRPLStreamURLIndex.Add(1)-1) % len(urls)
		url := urls[idx]
		logger.Log.Info().Str("url", url).Int("attempt", attempt).Msg("Attempting to connect XRPL client")

		// Close old client if exists before creating new one
		if XrplClient != nil {
			oldClient := XrplClient
			XrplClient = nil
			// Close in background; recover so nil/invalid conn in xrpl-go doesn't panic
			go func() {
				defer func() {
					if r := recover(); r != nil {
						logger.Log.Debug().Interface("recover", r).Msg("Recovered from panic while closing old XRPL client")
					}
				}()
				if err := oldClient.Close(); err != nil {
					logger.Log.Debug().Err(err).Msg("Error closing old XRPL client during reconnection")
				}
			}()
		}

		XrplClient = xrpl.NewClient(xrpl.ClientConfig{URL: url})

		// Check if client was created successfully
		if XrplClient == nil {
			logger.Log.Warn().Str("url", url).Int("attempt", attempt).Msg("Failed to create XRPL client (nil returned)")
			time.Sleep(backoff)
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			continue
		}

		err := safePingXRPL(XrplClient, []byte(url), fmt.Sprintf("connect url=%s attempt=%d", url, attempt))

		if err == nil {
			logger.Log.Info().Str("url", url).Int("attempt", attempt).Msg("Successfully connected XRPL client")
			return
		}

		logger.Log.Warn().Str("url", url).Int("attempt", attempt).Dur("retry_in", backoff).Err(err).Msg("XRPL connect failed; retrying infinitely")
		time.Sleep(backoff)
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

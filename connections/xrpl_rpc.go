package connections

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/xrpl-go"
)

var xrplRPCReconnectInProgress atomic.Bool

// XrplRPCClient is used for request/response (RPC) calls so that
// heavy requests do not contend with the streaming client.
var XrplRPCClient *xrpl.Client

// NewXrplRPCClient initializes RPC client using full-history URL if available
func NewXrplRPCClient() {
	url := config.EnvXrplWebsocketFullHistoryURL()
	if url == "" {
		url = config.EnvXrplWebsocketURL()
	}
	NewXrplRPCClientWithURL(url)
}

func NewXrplRPCClientWithURL(URL string) {
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
		url := urls[(attempt-1)%len(urls)]
		logger.Log.Info().Str("url", url).Int("attempt", attempt).Msg("Attempting to connect XRPL RPC client")

		// Close old client if exists before creating new one
		if XrplRPCClient != nil {
			// Close in background to avoid blocking
			go func() {
				if err := XrplRPCClient.Close(); err != nil {
					logger.Log.Debug().Err(err).Msg("Error closing old XRPL RPC client during reconnection")
				}
			}()
		}

		XrplRPCClient = xrpl.NewClient(xrpl.ClientConfig{URL: url})

		// Check if client was created successfully
		if XrplRPCClient == nil {
			logger.Log.Warn().Str("url", url).Int("attempt", attempt).Msg("Failed to create XRPL RPC client (nil returned)")
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
						Str("url", url).
						Int("attempt", attempt).
						Msg("Panic occurred during XRPL RPC client ping - connection may be nil")
				}
			}()
			err = XrplRPCClient.Ping([]byte(url))
		}()

		if err == nil {
			logger.Log.Info().Str("url", url).Int("attempt", attempt).Msg("Successfully connected XRPL RPC client")
			return
		}

		// Check if it's an IP limit error
		if isIPLimitError(err) {
			logger.Log.Warn().Str("url", url).Int("attempt", attempt).Err(err).Msg("IP limit reached during initial connection, waiting 5 minutes")
			time.Sleep(5 * time.Minute)
			backoff = time.Second // Reset backoff after long wait
		} else {
			logger.Log.Warn().Str("url", url).Int("attempt", attempt).Dur("retry_in", backoff).Err(err).Msg("XRPL RPC connect failed; retrying infinitely")
			time.Sleep(backoff)
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}
	}
}

// GetXRPLRequestClient returns preferred client for RPC requests
func GetXRPLRequestClient() *xrpl.Client {
	if XrplRPCClient != nil {
		return XrplRPCClient
	}
	return XrplClient
}

// CheckXRPLRPCConnectionHealth checks if the RPC client connection is healthy
func CheckXRPLRPCConnectionHealth() error {
	if XrplRPCClient == nil {
		return fmt.Errorf("XRPL RPC client is not initialized")
	}

	err := safePingXRPL(XrplRPCClient, []byte("health_check"), "rpc health check")

	if err != nil {
		logger.Log.Warn().Err(err).Msg("XRPL RPC client health check failed")
		return err
	}

	logger.Log.Debug().Msg("XRPL RPC client health check passed")
	return nil
}

// ReconnectXRPLRPCClient reconnects the RPC client with infinite retries
func ReconnectXRPLRPCClient(URL string) error {
	logger.Log.Info().Str("url", URL).Msg("Starting infinite retry reconnection for XRPL RPC client")

	// Close existing connection
	if XrplRPCClient != nil {
		CloseXrplRPCClient()
	}

	// Infinite retry loop with exponential backoff
	backoff := time.Second
	maxBackoff := 30 * time.Second
	attempt := 0
	urls := parseXRPLURLs(URL)
	if len(urls) == 0 {
		urls = []string{URL}
	}

	for {
		attempt++
		url := urls[(attempt-1)%len(urls)]
		logger.Log.Info().Str("url", url).Int("attempt", attempt).Msg("Attempting to reconnect XRPL RPC client")

		// Create new connection
		XrplRPCClient = xrpl.NewClient(xrpl.ClientConfig{URL: url})

		// Check if client was created successfully
		if XrplRPCClient == nil {
			logger.Log.Warn().Str("url", url).Int("attempt", attempt).Msg("Failed to create XRPL RPC client (nil returned) during reconnection")
			time.Sleep(backoff)
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			continue
		}

		err := safePingXRPL(XrplRPCClient, []byte("reconnect_test"), fmt.Sprintf("rpc reconnect url=%s attempt=%d", url, attempt))

		if err == nil {
			logger.Log.Info().Str("url", url).Int("attempt", attempt).Msg("Successfully reconnected XRPL RPC client")
			return nil
		}

		// Check if it's an IP limit error
		if isIPLimitError(err) {
			logger.Log.Warn().Str("url", url).Int("attempt", attempt).Err(err).Msg("IP limit reached during reconnection, waiting 5 minutes")
			time.Sleep(5 * time.Minute)
			backoff = time.Second // Reset backoff after long wait
		} else {
			logger.Log.Warn().Str("url", url).Int("attempt", attempt).Dur("retry_in", backoff).Err(err).Msg("XRPL RPC reconnect failed; retrying infinitely")
			time.Sleep(backoff)
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}
	}
}

// EnsureXRPLRPCConnected reconnects the RPC client when health check fails.
// Uses atomic to prevent concurrent reconnects from multiple goroutines.
// Blocks until reconnection succeeds.
func EnsureXRPLRPCConnected() {
	if !xrplRPCReconnectInProgress.CompareAndSwap(false, true) {
		logger.Log.Debug().Msg("XRPL RPC reconnect already in progress, waiting")
		// Wait for in-progress reconnect to finish by polling health
		for {
			if err := CheckXRPLRPCConnectionHealth(); err == nil {
				return
			}
			time.Sleep(2 * time.Second)
		}
	}
	defer xrplRPCReconnectInProgress.Store(false)

	url := config.EnvXrplWebsocketFullHistoryURL()
	if url == "" {
		url = config.EnvXrplWebsocketURL()
	}
	ReconnectXRPLRPCClient(url)
}

// isIPLimitError checks if the error is specifically about IP limit reached
func isIPLimitError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "close 1008") ||
		strings.Contains(errStr, "policy violation") ||
		strings.Contains(errStr, "IP limit reached")
}

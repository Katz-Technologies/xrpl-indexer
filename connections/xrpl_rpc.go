package connections

import (
	"time"

	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/xrpl-go"
)

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
	// Retry loop with exponential backoff until a successful ping
	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		XrplRPCClient = xrpl.NewClient(xrpl.ClientConfig{URL: URL})
		err := XrplRPCClient.Ping([]byte(URL))
		if err == nil {
			logger.Log.Info().Str("url", URL).Msg("Connected XRPL RPC client")
			return
		}

		logger.Log.Warn().Str("url", URL).Dur("retry_in", backoff).Err(err).Msg("XRPL RPC connect failed; retrying")
		time.Sleep(backoff)
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
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

package connections

import (
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
	// Retry loop with exponential backoff until a successful ping
	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		XrplClient = xrpl.NewClient(xrpl.ClientConfig{URL: URL})
		err := XrplClient.Ping([]byte(URL))
		if err == nil {
			logger.Log.Info().Str("url", URL).Msg("Connected to XRPL server")
			return
		}

		logger.Log.Warn().Str("url", URL).Dur("retry_in", backoff).Err(err).Msg("XRPL connect failed; retrying")
		time.Sleep(backoff)
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

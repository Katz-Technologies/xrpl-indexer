package connections

import (
	"fmt"
	"strings"

	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/xrpl-go"
)

func parseXRPLURLs(raw string) []string {
	parts := strings.Split(raw, ",")
	urls := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		urls = append(urls, trimmed)
	}
	if len(urls) == 0 {
		return []string{raw}
	}
	return urls
}

func safePingXRPL(client *xrpl.Client, payload []byte, context string) (err error) {
	if client == nil {
		return fmt.Errorf("XRPL client is nil")
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic during ping: %v", r)
			event := logger.Log.Error().Interface("panic", r)
			if context != "" {
				event = event.Str("context", context)
			}
			event.Msg("Panic occurred during XRPL client ping - connection may be nil")
		}
	}()

	return client.Ping(payload)
}

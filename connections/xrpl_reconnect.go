package connections

import (
	"sync/atomic"

	"github.com/xrpscan/platform/logger"
)

var xrplReconnectInProgress atomic.Bool

func ReconnectXRPL(err error, context string) {
	if !xrplReconnectInProgress.CompareAndSwap(false, true) {
		event := logger.Log.Debug()
		if context != "" {
			event = event.Str("context", context)
		}
		event.Msg("XRPL reconnect already in progress")
		return
	}
	defer xrplReconnectInProgress.Store(false)

	event := logger.Log.Warn()
	if err != nil {
		event = event.Err(err)
	}
	if context != "" {
		event = event.Str("context", context)
	}
	event.Msg("Reconnecting XRPL client")

	CloseXrplClient()
	NewXrplClient()
	SubscribeStreams()
}

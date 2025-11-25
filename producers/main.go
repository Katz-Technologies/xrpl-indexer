package producers

import (
	"encoding/json"
	"sync/atomic"

	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/models"
)

var lastSeenLedgerIndex uint32

func RunProducers() {
	for {
		select {
		case ledger := <-connections.XrplClient.StreamLedger:
			// Log each realtime ledger arrival to console
			var ls models.LedgerStream
			if err := json.Unmarshal(ledger, &ls); err == nil && ls.Type == models.LEDGER_STREAM_TYPE {
				logger.Log.Info().
					Uint32("ledger_index", ls.LedgerIndex).
					Str("ledger_hash", ls.LedgerHash).
					Uint32("txn_count", ls.TxnCount).
					Msg("New ledger closed")

				// Detect gaps and backfill if we skipped indices due to reconnects
				prev := atomic.LoadUint32(&lastSeenLedgerIndex)
				if prev != 0 && ls.LedgerIndex > prev+1 {
					// Backfill missing range (prev+1 .. ls.LedgerIndex-1)
					// Note: backfillMissingRange does NOT emit SocketIO events
					go backfillMissingRange(int(prev+1), int(ls.LedgerIndex-1))
				}
				atomic.StoreUint32(&lastSeenLedgerIndex, ls.LedgerIndex)
			} else {
				logger.Log.Info().Msg("New ledger message")
			}

			// Process transactions directly and write to ClickHouse
			// Pass isRealtime=true to emit SocketIO events for real-time transactions
			go ProcessTransactionsDirectly(ledger, true)

		case <-connections.XrplClient.StreamValidation:
			// ignore
		case <-connections.XrplClient.StreamPeerStatus:
			// ignore
		case <-connections.XrplClient.StreamConsensus:
			// ignore
		case <-connections.XrplClient.StreamPathFind:
			// ignore
		case <-connections.XrplClient.StreamManifest:
			// ignore
		case <-connections.XrplClient.StreamServer:
			// ignore
		case <-connections.XrplClient.StreamDefault:
			// ignore
		}
	}
}

// backfillMissingRange replays missing ledgers by processing transactions directly
// Note: This function does NOT emit SocketIO events (isRealtime=false)
func backfillMissingRange(from int, to int) {
	if to < from {
		return
	}
	logger.Log.Info().Int("from", from).Int("to", to).Int("count", to-from+1).Msg("Backfilling missing ledger range")
	for i := from; i <= to; i++ {
		ledger := models.LedgerStream{Type: models.LEDGER_STREAM_TYPE, LedgerIndex: uint32(i)}
		b, _ := json.Marshal(ledger)
		// Process transactions directly with isRealtime=false (no SocketIO events)
		go ProcessTransactionsDirectly(b, false)
	}
}

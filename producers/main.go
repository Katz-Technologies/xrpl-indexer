package producers

import (
	"context"
	"encoding/json"
	"log"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/xrpscan/platform/config"
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
					go backfillMissingRange(int(prev+1), int(ls.LedgerIndex-1))
				}
				atomic.StoreUint32(&lastSeenLedgerIndex, ls.LedgerIndex)
			} else {
				logger.Log.Info().Msg("New ledger message")
			}

			// Only produce transactions to avoid creating extra topics
			go ProduceTransactions(connections.KafkaWriter, ledger)

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

// backfillMissingRange replays missing ledgers by reusing the backfill producer flow
func backfillMissingRange(from int, to int) {
	if to < from {
		return
	}
	logger.Log.Info().Int("from", from).Int("to", to).Int("count", to-from+1).Msg("Backfilling missing ledger range")
	for i := from; i <= to; i++ {
		ledger := models.LedgerStream{Type: models.LEDGER_STREAM_TYPE, LedgerIndex: uint32(i)}
		b, _ := json.Marshal(ledger)
		// Produce ledger and transactions in order
		go ProduceLedger(connections.KafkaWriter, b)
		go ProduceTransactions(connections.KafkaWriter, b)
	}
}

func Produce(w *kafka.Writer, message []byte, topic string) {
	messageKey := uuid.NewString()
	if topic == "" {
		topic = config.TopicDefault()
	}

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Topic: topic,
			Key:   []byte(messageKey),
			Value: message,
		},
	)
	if err != nil {
		log.Printf("Failed to write message: %s", err)
	}
}

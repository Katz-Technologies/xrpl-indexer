package producers

import (
	"context"
	"encoding/json"
	"fmt"
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

			// Run producers asynchronously to avoid blocking the stream loop
			go ProduceLedger(connections.KafkaWriter, ledger)
			go ProduceTransactions(connections.KafkaWriter, ledger)

		case validation := <-connections.XrplClient.StreamValidation:
			go ProduceValidation(connections.KafkaWriter, validation)

		case peerStatus := <-connections.XrplClient.StreamPeerStatus:
			Produce(connections.KafkaWriter, peerStatus, config.TopicPeerStatus())

		case consensus := <-connections.XrplClient.StreamConsensus:
			Produce(connections.KafkaWriter, consensus, config.TopicConsensus())

		case pathFind := <-connections.XrplClient.StreamPathFind:
			Produce(connections.KafkaWriter, pathFind, config.TopicPathFind())

		case manifest := <-connections.XrplClient.StreamManifest:
			Produce(connections.KafkaWriter, manifest, config.TopicManifests())

		case server := <-connections.XrplClient.StreamServer:
			Produce(connections.KafkaWriter, server, config.TopicServer())

		case defaultObject := <-connections.XrplClient.StreamDefault:
			fmt.Println(string(defaultObject))
			Produce(connections.KafkaWriter, defaultObject, config.TopicDefault())
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

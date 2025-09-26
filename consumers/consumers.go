package consumers

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/indexer"
	"github.com/xrpscan/platform/logger"
)

// Serial consumer (based on callback function) for low volume message streams
func RunConsumer(conn *kafka.Reader, callback func(m kafka.Message)) {
	ctx := context.Background()
	for {
		m, err := conn.FetchMessage(ctx)
		if err != nil {
			break
		}
		callback(m)

		if err := conn.CommitMessages(ctx, m); err != nil {
			logger.Log.Error().Err(err).Msg("Failed to commit kafka message")
		}
	}
}

// Bulk message consumer (based on channel) for high volume message streams
func RunBulkConsumer(conn *kafka.Reader, callback func(<-chan kafka.Message)) {
	ctx := context.Background()
	ch := make(chan kafka.Message)
	go callback(ch)

	for {
		m, err := conn.FetchMessage(ctx)
		if err != nil {
			break
		}

		ch <- m

		if err := conn.CommitMessages(ctx, m); err != nil {
			logger.Log.Error().Err(err).Msg("Failed to commit kafka message")
		}
	}
}

// Run all consumers
func RunConsumers() {
	// Only run the transaction transformer; drop other noisy consumers
	go RunBulkConsumer(connections.KafkaReaderTransaction, func(ch <-chan kafka.Message) {
		ctx := context.Background()
		for {
			m := <-ch
			var tx map[string]interface{}
			if err := json.Unmarshal(m.Value, &tx); err != nil {
				logger.Log.Error().Err(err).Msg("Transaction json.Unmarshal error")
				continue
			}
			// Filter by allowed transaction types at code level
			if tt, ok := tx["TransactionType"].(string); ok {
				switch tt {
				case "Payment", "OfferCreate", "OfferCancel", "TrustSet":
					// allowed
				default:
					// skip unwanted transaction types
					continue
				}
			} else {
				// unknown or missing type; skip
				continue
			}
			modified, err := indexer.ModifyTransaction(tx)
			if err != nil {
				logger.Log.Error().Err(err).Msg("Error fixing transaction object")
				continue
			}
			b, err := json.Marshal(modified)
			if err != nil {
				logger.Log.Error().Err(err).Msg("Transaction json.Marshal error")
				continue
			}
			if err := connections.KafkaWriter.WriteMessages(ctx, kafka.Message{
				Topic: config.TopicTransactionsProcessed(),
				Key:   m.Key,
				Value: b,
			}); err != nil {
				logger.Log.Error().Err(err).Msg("Failed to write processed tx")
			}
		}
	})
}

package main

import (
	"encoding/json"
	"log"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/pkg/errors"
)

// Handler for log-Messages
type logHandler struct{}

func (*logHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Initializing Kafka LogHandler")
	return nil
}

func (*logHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Closing Kafka LogHandler")
	return nil
}

func (m *logHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	log.Println("LogHandler listening for log-messages...")
	for msg := range claim.Messages() {
		session.MarkMessage(msg, "")

		l := &model.LogEntry{}
		err := json.Unmarshal(msg.Value, l)
		if err != nil {
			err = errors.Wrap(err, "LogConsumer Error: Error unmarshalling log")
		}

		log.Printf(
			"%s: %d: %s: %s: %s: %s",
			l.Level,
			l.ErrorCode,
			l.ServiceName,
			l.EventAction,
			l.ServiceAction,
			l.Description,
		)
	}
	return errors.New("log-handler exited")
}

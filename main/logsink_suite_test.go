package main

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/TerrexTech/uuuid"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestLogSink(t *testing.T) {
	err := loadAndValidateEnv()
	if err != nil {
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "LogSink Suite")
}

// Log-message handler for testing
type msgHandler struct {
	msgCallback func(*sarama.ConsumerMessage) bool
}

func (*msgHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Initializing Kafka MsgHandler")
	return nil
}

func (*msgHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Closing Kafka MsgHandler")
	return nil
}

func (m *msgHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	if m.msgCallback == nil {
		return errors.New("msgCallback cannot be nil")
	}
	for msg := range claim.Messages() {
		session.MarkMessage(msg, "")

		val := m.msgCallback(msg)
		if val {
			return nil
		}
	}
	return errors.New("required value not found")
}

var _ = Describe("LogSink", func() {
	var (
		consumer *kafka.Consumer
		producer *kafka.Producer
		topic    string
	)

	BeforeSuite(func() {
		kafkaBrokers := os.Getenv("KAFKA_BROKERS")
		cGroup := os.Getenv("KAFKA_LOG_CONSUMER_GROP")
		topic = os.Getenv("KAFKA_LOG_CONSUMER_TOPIC")

		var err error

		consumer, err = kafka.NewConsumer(&kafka.ConsumerConfig{
			GroupName:    cGroup,
			KafkaBrokers: *commonutil.ParseHosts(kafkaBrokers),
			Topics:       []string{topic},
		})
		Expect(err).ToNot(HaveOccurred())

		producer, err = kafka.NewProducer(&kafka.ProducerConfig{
			KafkaBrokers: *commonutil.ParseHosts(kafkaBrokers),
		})
		Expect(err).ToNot(HaveOccurred())
		uuid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())

		testMsg := uuid.String()
		producer.Input() <- kafka.CreateMessage(topic, []byte(testMsg))
	})

	It("should consume logs", func(done Done) {
		uuid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		testMsg := uuid.String()
		producer.Input() <- kafka.CreateMessage(topic, []byte(testMsg))

		msgCallback := func(msg *sarama.ConsumerMessage) bool {
			defer GinkgoRecover()
			log.Println("A Response was received on response channel")
			v := string(msg.Value)

			if v == testMsg {
				log.Println("The response matches")
				close(done)
				return true
			}
			return false
		}

		handler := &msgHandler{msgCallback}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		consumer.Consume(ctx, handler)
	}, 20)
})

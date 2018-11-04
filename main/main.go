package main

import (
	"context"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

func loadAndValidateEnv() error {
	log.Println("Reading environment file")
	err := godotenv.Load("../.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",
		"KAFKA_LOG_CONSUMER_GROP",
		"KAFKA_LOG_CONSUMER_TOPIC",
	)
	if err != nil {
		err = errors.Wrapf(err, `Env-var "%s" is required, but is not set`, missingVar)
		return err
	}

	return nil
}

func main() {
	err := loadAndValidateEnv()
	if err != nil {
		log.Fatalln(err)
	}

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	cGroup := os.Getenv("KAFKA_LOG_CONSUMER_GROP")
	cTopic := os.Getenv("KAFKA_LOG_CONSUMER_TOPIC")

	consumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		GroupName:    cGroup,
		KafkaBrokers: *commonutil.ParseHosts(kafkaBrokers),
		Topics:       []string{cTopic},
	})
	if err != nil {
		err = errors.Wrap(err, "Error creating log-consumer")
		log.Fatalln(err)
	}

	go func() {
		for err := range consumer.Errors() {
			err = errors.Wrap(err, "Error in Log-Consumer")
			log.Fatalln(err)
		}
	}()
	err = initConsumer(consumer, &logHandler{})
	if err != nil {
		log.Fatalln(err)
	}
}

func initConsumer(c *kafka.Consumer, handler sarama.ConsumerGroupHandler) error {
	ctx := context.Background()
	err := c.Consume(ctx, &logHandler{})
	if err != nil {
		err = errors.Wrap(err, "Error while consuming log-messages")
		return err
	}
	return nil
}

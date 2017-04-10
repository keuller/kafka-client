package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

const LIMIT = 3

var (
	producer sarama.SyncProducer
)

func main() {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 2                    // Retry up to 2 times to produce the message
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)

	if err != nil {
		log.Fatalln("Failed to start producer:", err)
	}

	println("Golang Kafka producer is ready!")

	for idx := 1; idx <= LIMIT; idx++ {
		_, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: "sample",
			Value: sarama.StringEncoder(fmt.Sprintf("Message %d from golang.", idx)),
		})

		if err != nil {
			log.Fatalf(err.Error())
		}

		time.Sleep(1500 * time.Millisecond)
	}

}

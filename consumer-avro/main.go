package main

import (
	"bytes"
	"encoding/binary"
	"log"

	"github.com/illenko/kafka/common-avro/avro"
	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	kafkaBroker       = "localhost"
	groupID           = "transaction-consumer"
	autoOffsetReset   = "earliest"
	schemaRegistryURL = "http://localhost:8081"
	topic             = "transaction-events"
)

func main() {
	consumer, err := setupKafkaConsumer(kafkaBroker, groupID, autoOffsetReset)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	schemaRegistryClient := setupSchemaRegistryClient(schemaRegistryURL)

	consumeMessages(consumer, schemaRegistryClient)
}

func setupKafkaConsumer(broker, groupID, offsetReset string) (*kafka.Consumer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          groupID,
		"auto.offset.reset": offsetReset,
	}
	return kafka.NewConsumer(config)
}

func setupSchemaRegistryClient(url string) *srclient.SchemaRegistryClient {
	return srclient.NewSchemaRegistryClient(url)
}

func consumeMessages(consumer *kafka.Consumer, schemaRegistryClient *srclient.SchemaRegistryClient) {
	consumer.SubscribeTopics([]string{topic}, nil)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			processMessage(msg, schemaRegistryClient)
		} else {
			log.Printf("Error consuming the message: %v (%v)\n", err, msg)
		}
	}
}

func processMessage(msg *kafka.Message, schemaRegistryClient *srclient.SchemaRegistryClient) {
	schemaID := binary.BigEndian.Uint32(msg.Value[1:5])
	schema, err := schemaRegistryClient.GetSchema(int(schemaID))
	if err != nil {
		log.Printf("Error getting the schema with id '%d': %v", schemaID, err)
		return
	}

	event, err := avro.DeserializeTransactionEventFromSchema(bytes.NewReader(msg.Value[5:]), schema.Schema())
	if err != nil {
		log.Printf("Error deserializing the message: %v\n", err)
		return
	}

	log.Printf("Received message: %v\n", event)
}

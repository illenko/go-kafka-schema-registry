package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/illenko/kafka/common-avro/avro"
	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	kafkaBroker       = "localhost:9092"
	schemaRegistryURL = "http://localhost:8081"
	schemaFile        = "transaction_event.avsc"
)

func main() {
	topic := "transaction-events"
	producer, err := setupKafkaProducer(kafkaBroker)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	schema, err := getSchema(schemaRegistryURL, topic, schemaFile)
	if err != nil {
		log.Fatalf("Failed to get schema: %v", err)
	}

	go handleProducerEvents(producer)

	for i := 0; i < 10; i++ {
		event := generateRandomEvent()
		recordValue, err := serializeEvent(schema, event)
		if err != nil {
			log.Fatalf("Failed to serialize event: %v", err)
		}

		key, _ := uuid.NewUUID()
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(key.String()),
			Value: recordValue,
		}, nil)
	}

	producer.Flush(15 * 1000)
}

func setupKafkaProducer(broker string) (*kafka.Producer, error) {
	return kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
}

func getSchema(schemaRegistryURL, topic, schemaFile string) (*srclient.Schema, error) {
	client := srclient.NewSchemaRegistryClient(schemaRegistryURL)
	schema, err := client.GetLatestSchema(topic)
	if schema == nil {
		schemaBytes, err := os.ReadFile(schemaFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read schema file: %v", err)
		}
		schema, err = client.CreateSchema(topic, string(schemaBytes), srclient.Avro)
		if err != nil {
			return nil, fmt.Errorf("failed to create schema: %v", err)
		}
	}
	return schema, err
}

func handleProducerEvents(producer *kafka.Producer) {
	for event := range producer.Events() {
		switch ev := event.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Error delivering the message '%s'\n", ev.Key)
			} else {
				log.Printf("Message '%s' delivered successfully!\n", ev.Key)
			}
		}
	}
}

func generateRandomEvent() avro.TransactionEvent {
	return avro.TransactionEvent{
		TransactionId: uuid.New().String(),
		UserId:        uuid.New().String(),
		Amount:        rand.Float64() * 1000,
		Currency:      "USD",
		Timestamp:     time.Now().Unix(),
		Status:        "completed",
		EventType:     "transaction",
	}
}

func serializeEvent(schema *srclient.Schema, event avro.TransactionEvent) ([]byte, error) {
	var buf bytes.Buffer
	err := event.Serialize(&buf)
	if err != nil {
		return nil, err
	}

	valueBytes := buf.Bytes()

	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))

	var recordValue []byte
	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, schemaIDBytes...)
	recordValue = append(recordValue, valueBytes...)

	return recordValue, nil
}

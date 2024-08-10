module github.com/illenko/kafka/producer-avro

go 1.22.3

replace github.com/illenko/kafka/common-avro/avro => ../common-avro

require (
	github.com/google/uuid v1.6.0
	github.com/illenko/kafka/common-avro/avro v0.0.0-00010101000000-000000000000
	github.com/riferrei/srclient v0.7.0
	gopkg.in/confluentinc/confluent-kafka-go.v1 v1.8.2
)

require (
	github.com/actgardner/gogen-avro/v10 v10.2.1 // indirect
	github.com/confluentinc/confluent-kafka-go v1.9.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/linkedin/goavro/v2 v2.12.0 // indirect
	github.com/santhosh-tekuri/jsonschema/v5 v5.0.0 // indirect
	github.com/stretchr/testify v1.9.0 // indirect
	golang.org/x/sync v0.7.0 // indirect
)

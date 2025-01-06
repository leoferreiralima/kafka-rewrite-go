package main

import (
	"github.com/codecrafters-io/kafka-starter-go/app/handlers"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/codecrafters-io/kafka-starter-go/app/server"
)

func main() {
	kafkaServer := server.NewKafkaServer()

	kafkaServer.HandlerFunc(server.KafkaApiKey{
		ApiKey:     protocol.ApiVersions,
		MinVersion: 0,
		MaxVersion: 4,
	},
		handlers.ApiVersionsHandler,
		nil,
	)

	kafkaServer.HandlerFunc(server.KafkaApiKey{
		ApiKey:     protocol.DescribeTopicPartitions,
		MinVersion: 0,
		MaxVersion: 0,
	},
		handlers.DescribeTopicPartitionsHandler,
		&server.HandlerOpts{
			Request: server.HandlerRequestOpts{
				Version: 2,
			},
			Response: server.HandlerResponseOpts{
				Version: 1,
			},
		},
	)

	err := kafkaServer.ListenAndServe(":9092")

	if err != nil {
		panic(err)
	}
}

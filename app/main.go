package main

import (
	"github.com/codecrafters-io/kafka-starter-go/app/handlers"
	"github.com/codecrafters-io/kafka-starter-go/app/server"
)

func main() {
	kafkaServer := server.NewKafkaServer()

	kafkaServer.
		Handler(server.ApiVersions).
		Version(0, 4).Add(handlers.ApiVersionsHandler)

	kafkaServer.
		Handler(server.DescribeTopicPartitions).
		Version(0, 0).
		Opts().
		ResponseHeaderVersion(1).
		And().
		Add(handlers.DescribeTopicPartitionsHandler)

	err := kafkaServer.ListenAndServe(":9092")

	if err != nil {
		panic(err)
	}
}

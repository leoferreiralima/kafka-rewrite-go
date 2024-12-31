package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/kafka"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	request, err := kafka.ParseRequest(conn)

	if err != nil {
		fmt.Println("Error parsing request: ", err.Error())
		panic(err)
	}

	fmt.Printf("Request\n%d\n%x\n", request.MessageSize, request.CorrelationId)

	response := kafka.NewResponse()
	response.CorrelationId = request.CorrelationId

	if err = response.Write(conn); err != nil {
		fmt.Println("Error writing response: ", err.Error())
		panic(err)
	}

	fmt.Printf("Response sended \n%x\n", response.CorrelationId)
}

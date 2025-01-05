package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/server"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go server.HandleConnection(conn)
	}

}

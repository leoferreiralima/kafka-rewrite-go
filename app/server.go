package main

import (
	"fmt"
	"net"
	"os"
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

	message_size := make([]byte, 4)
	header := make([]byte, 4)
	header[3] = 7

	if _, err = conn.Write(message_size); err != nil {
		panic(err)
	}

	if _, err = conn.Write(header); err != nil {
		panic(err)
	}
}

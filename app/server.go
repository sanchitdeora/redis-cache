package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port :6379")
		os.Exit(1)
	}

	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
	
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	
	buf := make([]byte, 128)
	for {

		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}
		if n == 0 {
			fmt.Println("buffer is empty")
			return
		}
	
		fmt.Printf("Message Received: %s", buf[:n])
	
		if strings.Contains(string(buf[:n]), "ping") {
			_, err = conn.Write([]byte("+PONG\r\n"))
		}
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			return
		}
	}
}

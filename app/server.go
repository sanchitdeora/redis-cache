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
	

		// parsing redis-like input protocols

		requestLines := strings.Split(string(buf[:n]), CLRF)
		if len(requestLines) < 3 {
			fmt.Println("invalid command received:", requestLines)
			return
		}
		
		command := Commands(strings.ToUpper(requestLines[2]))

		var response string
		switch command {
			case PING:
				response = "+PONG\r\n"
			case ECHO:
				if len(requestLines) < 5 {
					fmt.Println("invalid command received for ECHO:", requestLines)
					return
				}
				response = BuildResponse(requestLines[4])
			default:
				response = "-ERR unknown command\r\n"
				fmt.Println("invalid command received:", command)
		}

		_, err = conn.Write([]byte(response))
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			return
		}
	}
}

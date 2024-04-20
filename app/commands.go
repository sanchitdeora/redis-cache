package main

import (
	"fmt"
	"strings"
)

var keyValueStore = make(map[string]string)

type Commands string

const (
	CLRF string = "\r\n"

	PING Commands = "PING"
	ECHO Commands = "ECHO"
	GET Commands = "GET"
	SET Commands = "SET"
)

// parsing redis-like input protocols
func ParseCommands(buffer []byte, readLen int) (string, error) {			
	requestLines := strings.Split(string(buffer[:readLen]), CLRF)
	if len(requestLines) < 3 {
		return "", fmt.Errorf("invalid command received: %s", requestLines)
	}

	command := Commands(strings.ToUpper(requestLines[2]))

	switch command {
		case PING:
			return PingHandler()
		
		case ECHO:
			return EchoHandler(requestLines)
		
		case SET:
			return SetHandler(requestLines)
		
		case GET:
			return GetHandler(requestLines)

		default:
			return "", fmt.Errorf("invalid command received: %s", command)
	}
}

// Command Handlers
func PingHandler() (string, error) {
	return "+PONG\r\n", nil
}

func EchoHandler(requestLines []string) (string, error) {
	if len(requestLines) < 5 {
		return "", fmt.Errorf("invalid command received. ECHO should have one arguments: %s", requestLines)
	}

	return buildResponse(requestLines[4]), nil
}

func SetHandler(requestLines []string) (string, error) {
	if len(requestLines) < 7 {
		return "", fmt.Errorf("invalid command received. SET should have more arguments: %s", requestLines)
	}
	
	keyValueStore[requestLines[4]] = requestLines[6]

	return "+OK\r\n", nil
}

func GetHandler(requestLines []string) (string, error) {
	if len(requestLines) < 5 {
		return "", fmt.Errorf("invalid command received. GET should have more arguments: %s", requestLines)
	}

	val, exists := keyValueStore[requestLines[4]]; if !exists {
		return "$-1\r\n", nil
	}

	return buildResponse(val), nil
}


func buildResponse(message string) string {
	return fmt.Sprintf("$%v\r\n%s\r\n", len(message), message)
}
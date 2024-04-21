package main

import (
	"fmt"
	"strconv"
	"strings"
)

type Commands string

const (
	CLRF string = "\r\n"

	PING Commands = "PING"
	ECHO Commands = "ECHO"
	GET Commands = "GET"
	SET Commands = "SET"
	PX Commands = "PX"
)

type CommandsHandler struct {
	Store Store
}

func NewCommandsHandler() CommandsHandler{
	return CommandsHandler{
		Store: NewStore(),
	}
}


// parsing redis-like input protocols
func (ch *CommandsHandler) ParseCommands(buffer []byte, readLen int) (string, error) {			
	requestLines := strings.Split(string(buffer[:readLen]), CLRF)
	if len(requestLines) < 3 {
		return "", fmt.Errorf("invalid command received: %s", requestLines)
	}

	command := Commands(strings.ToUpper(requestLines[2]))

	switch command {
		case PING:
			return ch.PingHandler()
		
		case ECHO:
			return ch.EchoHandler(requestLines)
		
		case SET:
			return ch.SetHandler(requestLines)
		
		case GET:
			return ch.GetHandler(requestLines)

		default:
			return "", fmt.Errorf("invalid command received: %s", command)
	}
}

// Command Handlers
func (ch *CommandsHandler) PingHandler() (string, error) {
	return "+PONG\r\n", nil
}

func (ch *CommandsHandler) EchoHandler(requestLines []string) (string, error) {
	if len(requestLines) < 5 {
		return "", fmt.Errorf("invalid command received. ECHO should have one arguments: %s", requestLines)
	}

	return buildResponse(requestLines[4]), nil
}

func (ch *CommandsHandler) SetHandler(requestLines []string) (string, error) {
	if len(requestLines) < 7 {
		return "", fmt.Errorf("invalid command received. SET should have more arguments: %s", requestLines)
	}

	var expiration int64 = -1
	if len(requestLines) > 11 {
		command := Commands(strings.ToUpper(requestLines[8]))
		if command == PX {
			convertedExpiration, err := strconv.ParseInt(requestLines[10], 10, 64)
			if err != nil {
				return "", fmt.Errorf("")
			}
			expiration = convertedExpiration
		}
	}

	if err := ch.Store.Set(requestLines[4], requestLines[6], expiration); err != nil {
		return "", fmt.Errorf("error while setting in store: %s", err.Error())
	}

	return "+OK\r\n", nil
}

func (ch *CommandsHandler) GetHandler(requestLines []string) (string, error) {
	if len(requestLines) < 5 {
		return "", fmt.Errorf("invalid command received. GET should have more arguments: %s", requestLines)
	}

	val, err := ch.Store.Get(requestLines[4]);
	if err != nil {
		return "", fmt.Errorf("error while getting from store with key: %s %s", requestLines[4], err.Error())
	}
	if val == "" {
		return "$-1\r\n", nil
	}
	
	return buildResponse(val), nil
}

func buildResponse(message string) string {
	return fmt.Sprintf("$%v\r\n%s\r\n", len(message), message)
}
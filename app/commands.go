package main

import (
	"fmt"
)

type Commands string

const (
	CLRF string = "\r\n"

	PING Commands = "PING"
	ECHO Commands = "ECHO"
)

func BuildResponse(message string) string {
	return fmt.Sprintf("$%v\r\n%s\r\n", len(message), message)
}
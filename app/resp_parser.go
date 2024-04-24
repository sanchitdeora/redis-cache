package main

import (
	"fmt"
	"strconv"
	"strings"
)

type RESPType string
type RESPFirstChar string

const (
	CLRF string = "\r\n"

	// RESP Protocol CHARS
	SimpleStringsFirstChar = "+"
	BulkStringsFirstChar = "$"
	ArraysFirstChar = "*"
	NullsFirstChar = "-"
	IntegersFirstChar = ":"

	// RESP Protocol Type Names
	SimpleStringsRespType RESPType = "SimpleStrings"
	BulkStringsRespType RESPType = "BulkStrings"
	ArraysRespType RESPType = "Arrays"
	NullsRespType RESPType = "Nulls"
	IntegersRespType RESPType = "Integers"
)

// Responses

func OKResponse() []string {
	return []string{"+OK\r\n"}
}

func NullResponse() []string {
	return []string{"-1\r\n"}
}

func ResponseBuilder(respType RESPType, args ...string) (string, error) {
	switch respType {
		case SimpleStringsRespType:
			if len(args) > 1 {
				return "", fmt.Errorf("invalid response. simple strings cannot have more than one string")
			}
			return fmt.Sprintf("%s%s%s", SimpleStringsFirstChar, args[0], CLRF), nil

		case BulkStringsRespType:
			res := args[0]
			if len(args) > 1 {
				res = strings.Join(args, "\n")
			}
			return fmt.Sprintf("%s%v%s%s%s", BulkStringsFirstChar, len(res), CLRF, res, CLRF), nil
		}
	return "", nil
}

func ParseRequest(req string) ([]string, error) {
	// remove the empty string at the end
	requestLines := strings.Split(req, CLRF)
	requestLines = requestLines[0:len(requestLines)-1]

	err := requestValidator(requestLines)
	if err != nil {
		return nil, fmt.Errorf("invalid command received: %s", requestLines)
	}

	return requestLines, nil
}

func requestValidator(request []string) error {
	if len(request) < 3 {
		return fmt.Errorf("invalid request received: %s", request)
	}

	// should start with an asterisk (*) 
	if request[0][0:1] != ArraysFirstChar {
		return fmt.Errorf("request is not an array: %s", request)
	}

	// asterisk should be followed by the size of array
	arraySizeStr := request[0][1:]
	arraySize, err := strconv.Atoi(arraySizeStr)
	if err != nil {
		return fmt.Errorf("invalid array size received: %s", arraySizeStr)
	}

	var i int = 1
	if len(request[i:]) != (arraySize * 2) {
		return fmt.Errorf("array size does not match request size: %s", arraySizeStr)
	}

	// keeping it simple for now, only expect to have bulk strings inside input arrays
	for j := 0; j < arraySize; j ++ {
		if request[i][0:1] != BulkStringsFirstChar {
			return fmt.Errorf("content inside request is not a bulk string: %s", request)
		}

		bulkStringSizeStr := request[i][1:]
		bulkStringSize, err := strconv.Atoi(bulkStringSizeStr)
		if err != nil {
			return fmt.Errorf("invalid bulk string size received: %s", bulkStringSizeStr)
		}
		if len(request[i+1]) != bulkStringSize {
			return fmt.Errorf("string size does not match request size: %v, string: %s", bulkStringSize, request[i+1])
		}
		i += 2
	}

	return nil
}

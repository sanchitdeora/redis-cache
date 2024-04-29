package main

import (
	"fmt"
	"strconv"

	// "strconv"
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

func NoneTypeResponse() []string {
	return []string{"+none\r\n"}
}

func StringResponse() []string {
	return []string{"+string\r\n"}
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
		
		case ArraysRespType:
			if len(args) == 0 {
				return "", fmt.Errorf("invalid response. arrays cannot have zero arguments")
			}

			resp := fmt.Sprintf("%s%v%s", ArraysFirstChar, len(args), CLRF)
			for _, arg := range args {
				resp += fmt.Sprintf("%s%v%s%s%s", BulkStringsFirstChar, len(arg), CLRF, arg, CLRF)
			}
			return resp, nil
		}

	return "", nil
}

func ParseRequest(req string) (cReqs []string, err error) {
	requestLines := SplitRequests(req)

	for i := 0; i < len(requestLines); {
		cReq, nextIndex, err := FetchCommandRequest(requestLines, i)
		if err != nil {
			return cReqs, fmt.Errorf("error fetching commands from request: %s", err.Error())
		}

		cReqs = append(cReqs, cReq) 
		i = nextIndex
	}

	return cReqs, nil
}

func FetchCommandRequest(requestLines []string, startIndex int) (cReq string, nextIndex int, err error) {
	switch requestLines[startIndex][0:1] {
		
		case SimpleStringsFirstChar:
			cReq = requestLines[startIndex] + CLRF
			return cReq, startIndex + 1, nil

		case BulkStringsFirstChar:
			reqLength, err := strconv.Atoi(requestLines[startIndex][1:])
			if err != nil {
				return "", startIndex + 1, fmt.Errorf("error converting bulkstring length in request from str to int: %s", err.Error())
			}
			endIndex := startIndex + 1
			if reqLength != len(requestLines[endIndex]) {
				
				// Since RDB file is not a RESP response, it does not end with CLRF
				if IsRdbFile(requestLines[endIndex]) {
					overlappingReq := requestLines[endIndex][reqLength:]
					
					// add overlapping request after rdb file
					requestLines[endIndex] = requestLines[endIndex][0:reqLength]
					requestLines = append(requestLines[0:endIndex + 1], requestLines[endIndex:]...)
					requestLines[endIndex + 1] = overlappingReq
				} else {
					return "", startIndex + 1, fmt.Errorf("bulkstring length: %v in request does not match the following string: %s", reqLength, requestLines[startIndex + 1])
				}
			}
			cReq = requestLines[startIndex] + CLRF + requestLines[startIndex + 1]
			if !IsRdbFile(requestLines[startIndex + 1]) {
				cReq += CLRF
			}

			return cReq, startIndex + 2, nil

		case ArraysFirstChar:
			reqLength, err := strconv.Atoi(requestLines[startIndex][1:])
			if err != nil {
				return "", startIndex + 1, fmt.Errorf("error converting array length in request from str to int: %s", err.Error())
			}

			endIndex := startIndex + (reqLength * 2) + 1

			return strings.Join(requestLines[startIndex:endIndex], CLRF) + CLRF, endIndex, nil

		default:
			return "", startIndex + 1, fmt.Errorf("error parsing request. invalid first char received: %s", requestLines[startIndex][0:1])

	}
}

// func requestValidator(request []string) error {
// 	if len(request) < 3 {
// 		return fmt.Errorf("invalid request received: %s", request)
// 	}

// 	// should start with an asterisk (*) 
// 	if request[0][0:1] == ArraysFirstChar {
// 		request[0] = request[0][1:]
// 	}

// 	// asterisk should be followed by the size of array
// 	arraySizeStr := request[0][0:]
// 	arraySize, err := strconv.Atoi(arraySizeStr)
// 	if err != nil {
// 		return fmt.Errorf("invalid array size received: %s", arraySizeStr)
// 	}

// 	var i int = 1
// 	if len(request[i:]) != (arraySize * 2) {
// 		return fmt.Errorf("array size does not match request size: %s", arraySizeStr)
// 	}

// 	// keeping it simple for now, only expect to have bulk strings inside input arrays
// 	for j := 0; j < arraySize; j ++ {
// 		if request[i][0:1] != BulkStringsFirstChar {
// 			return fmt.Errorf("content inside request is not a bulk string: %s", request)
// 		}

// 		bulkStringSizeStr := request[i][1:]
// 		bulkStringSize, err := strconv.Atoi(bulkStringSizeStr)
// 		if err != nil {
// 			return fmt.Errorf("invalid bulk string size received: %s", bulkStringSizeStr)
// 		}
// 		if len(request[i+1]) != bulkStringSize {
// 			return fmt.Errorf("string size does not match request size: %v, string: %s", bulkStringSize, request[i+1])
// 		}
// 		i += 2
// 	}

// 	return nil
// }

func IsRdbFile(req string) bool {
	return strings.Contains(req, "REDIS")
}

func SplitRequests(req string) []string {
	requestLines := strings.Split(req, CLRF)

	// remove the empty string at the end
	if len(requestLines[len(requestLines) - 1]) == 0 {
		requestLines = requestLines[0:len(requestLines)-1]
	}
	
	return requestLines
}

func CombineRequests(reqs []string, isRespRequest bool) string {
	req := strings.Join(reqs, CLRF)
	if isRespRequest {
		req += CLRF
	}
	return req
}
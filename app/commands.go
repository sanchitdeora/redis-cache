package main

import (
	"fmt"
	"strconv"
	"strings"
)

type Command string

const (
	PING Command = "PING"
	ECHO Command = "ECHO"
	GET Command = "GET"
	SET Command = "SET"
	PX Command = "PX"
	INFO Command = "INFO"
	REPLCONF Command = "REPLCONF"
	PSYNC Command = "PSYNC"
	FULLRESYNC Command = "FULLRESYNC"

	// info response constants
	InfoRole = "role"
	InfoMasterReplicationID = "master_replid"
	InfoMasterReplicationOffset = "master_repl_offset"
)

type CommandOpts struct {
	ServerInfo ServerOpts
}

type Commands struct {
	CommandOpts
	Store Store
}

func NewCommandsHandler(opts CommandOpts) Commands{
	return Commands{
		CommandOpts: opts,
		Store: NewStore(),
	}
}

func IsWriteCommand(req string) bool {
	requestLines, err := ParseRequest(req) 
	if err != nil {
		fmt.Printf("error parsing request: %s", err.Error())
		return false
	}

	if Command(strings.ToUpper(requestLines[2])) == SET {
		return true
	}

	return false
}

func IsPsyncCommand(fullRequest string) bool {
	reqs, err := ParserMultiLineRequest(fullRequest)
	if err != nil {
		return false
	}

	for _, req := range reqs {
		requestLines, err := ParseRequest(req) 
		if err != nil {
			return false
		}
	
		if Command(strings.ToUpper(requestLines[2])) == PSYNC {
			return true
		}
	}
	return false
}

func CompletePartialRequest(req string) string {
	if req[0:1] != ArraysFirstChar {
		req = ArraysFirstChar + req[0:]
	}
	return req
}

// parsing redis-like input protocols
func (ch *Commands) ParseCommands(fullRequest string) ([]string, error) {			
	reqs, err := ParserMultiLineRequest(fullRequest)
	if err != nil {
		return nil, fmt.Errorf("error while parsing commands: %s", err.Error())
	}

	resList := make([]string, 0)
	for _, req := range reqs {
		requestLines, err := ParseRequest(req)	
		if err != nil {
			return nil, fmt.Errorf("error while parsing commands: %s", err.Error())
		}

		res, err := ch.CommandsHandler(requestLines)
		if err != nil {
			return nil, fmt.Errorf("error while parsing commands: %s", err.Error())
		}
		
		resList = append(resList, res...)
	}
	return resList, nil
}

func (ch *Commands) CommandsHandler(requestLines []string) ([]string, error) {

	command := Command(strings.ToUpper(requestLines[2]))
	
	switch command {
		case PING:
			return ch.PingHandler()

		case ECHO:
			return ch.EchoHandler(requestLines)

		case SET:
			return ch.SetHandler(requestLines)

		case GET:
			return ch.GetHandler(requestLines)

		case INFO:
			return ch.InfoHandler(requestLines)

		case REPLCONF:
			return ch.ReplConfHandler()

		case PSYNC:
			return ch.PsyncHandler()

		case FULLRESYNC:
			return ch.FullResyncHandler()

		default:
			return NullResponse(), fmt.Errorf("invalid command received: %s", command)
	}
}

// Command Handlers
func (ch *Commands) PingHandler() ([]string, error) {
	resp, err := ResponseBuilder(SimpleStringsRespType, "PONG")
	if err != nil {
		return nil, fmt.Errorf("error creating response: %s", err.Error())
	}
	return []string{resp}, nil
}

func (ch *Commands) EchoHandler(requestLines []string) ([]string, error) {
	if len(requestLines) < 5 {
		return nil, fmt.Errorf("invalid command received. ECHO should have one arguments: %s", requestLines)
	}

	resp, err := ResponseBuilder(BulkStringsRespType, requestLines[4])
	if err != nil {
		return nil, fmt.Errorf("error creating response: %s", err.Error())
	}
	return []string{resp}, nil
}

func (ch *Commands) SetHandler(requestLines []string) ([]string, error) {
	if len(requestLines) < 7 {
		return nil, fmt.Errorf("invalid command received. SET should have more arguments: %s", requestLines)
	}

	var expiration int64 = -1
	if len(requestLines) >= 11 {
		command := Command(strings.ToUpper(requestLines[8]))
		if command == PX {
			convertedExpiration, err := strconv.ParseInt(requestLines[10], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error while converting expiration time string arg to int64: %s", err.Error())
			}
			expiration = convertedExpiration
		}
	}

	if err := ch.Store.Set(requestLines[4], requestLines[6], expiration); err != nil {
		return nil, fmt.Errorf("error while setting in store: %s", err.Error())
	}

	if ch.ServerInfo.Role == RoleSlave {
		return []string{}, nil
	}

	return OKResponse(), nil
}

func (ch *Commands) GetHandler(requestLines []string) ([]string, error) {
	if len(requestLines) < 5 {
		return nil, fmt.Errorf("invalid command received. GET should have more arguments: %s", requestLines)
	}

	val, err := ch.Store.Get(requestLines[4]);
	if err != nil {
		return nil, fmt.Errorf("error while getting from store with key: %s %s", requestLines[4], err.Error())
	}
	if val == "" {
		return []string{"$-1\r\n"}, nil
	}

	resp, err := ResponseBuilder(BulkStringsRespType, val)
	if err != nil {
		return nil, fmt.Errorf("error creating response: %s", err.Error())
	}
	return []string{resp}, nil
}

func (ch *Commands) InfoHandler(requestLines []string) ([]string, error) {
	resp, err := ResponseBuilder(
		BulkStringsRespType,
		fmt.Sprintf("%s:%s", InfoRole, ch.ServerInfo.Role), 
		fmt.Sprintf("%s:%s", InfoMasterReplicationID, ch.ServerInfo.MasterReplicationID), 
		fmt.Sprintf("%s:%v", InfoMasterReplicationOffset, ch.ServerInfo.MasterReplicationOffset),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating response: %s", err.Error())
	}
	return []string{resp}, nil
}

func (ch *Commands) ReplConfHandler() ([]string, error) {
	return OKResponse(), nil
}

func (ch *Commands) PsyncHandler() ([]string, error) {
	rdb, err := ch.Store.ToRDBStore()
	if err != nil {
		return nil, fmt.Errorf("error getting raw rdb store: %s", err.Error())
	}

	return []string{
		fmt.Sprintf("+FULLRESYNC %s 0\r\n", ch.ServerInfo.MasterReplicationID),
		fmt.Sprintf("$%v\r\n%s", len(rdb), rdb),
	}, nil
}

func (ch *Commands) FullResyncHandler() ([]string, error) {
	return OKResponse(), nil
}

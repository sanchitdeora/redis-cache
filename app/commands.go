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
	INFO Commands = "INFO"
	REPLCONF Commands = "REPLCONF"
	PSYNC Commands = "PSYNC"
	FULLRESYNC Commands = "FULLRESYNC"

	// info response constants
	INFO_ROLE = "role"
	INFO_MASTER_REPLICATION_ID = "master_replid"
	INFO_MASTER_REPLICATION_OFFSET = "master_repl_offset"
)

type CommandOpts struct {
	ServerInfo ServerOpts
}

type CommandsHandler struct {
	CommandOpts
	Store Store
}

func NewCommandsHandler(opts CommandOpts) CommandsHandler{
	return CommandsHandler{
		CommandOpts: opts,
		Store: NewStore(),
	}
}


// parsing redis-like input protocols
func (ch *CommandsHandler) ParseCommands(buffer []byte, readLen int) ([]string, error) {			
	requestLines := strings.Split(string(buffer[:readLen]), CLRF)
	if len(requestLines) < 3 {
		return nil, fmt.Errorf("invalid command received: %s", requestLines)
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

		case INFO:
			return ch.InfoHandler(requestLines)

		case REPLCONF:
			return ch.ReplConfHandler()

		case PSYNC:
			return ch.PsyncHandler()

		case FULLRESYNC:
			return ch.FullResyncHandler()

		default:
			return []string{"$-1/r/n"}, fmt.Errorf("invalid command received: %s", command)
	}
}

// Command Handlers
func (ch *CommandsHandler) PingHandler() ([]string, error) {
	return []string{"+PONG\r\n"}, nil
}

func (ch *CommandsHandler) EchoHandler(requestLines []string) ([]string, error) {
	if len(requestLines) < 5 {
		return nil, fmt.Errorf("invalid command received. ECHO should have one arguments: %s", requestLines)
	}

	return []string{buildResponse(requestLines[4])}, nil
}

func (ch *CommandsHandler) SetHandler(requestLines []string) ([]string, error) {
	if len(requestLines) < 7 {
		return nil, fmt.Errorf("invalid command received. SET should have more arguments: %s", requestLines)
	}

	var expiration int64 = -1
	if len(requestLines) > 11 {
		command := Commands(strings.ToUpper(requestLines[8]))
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

	return []string{"+OK\r\n"}, nil
}

func (ch *CommandsHandler) GetHandler(requestLines []string) ([]string, error) {
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

	return []string{buildResponse(val)}, nil
}

func (ch *CommandsHandler) InfoHandler(requestLines []string) ([]string, error) {
	fmt.Printf("--- debug ServerInfo---\nRole:%s\nReplicationID:%s\nOffset:%v\n", ch.ServerInfo.Role, ch.ServerInfo.MasterReplicationID, ch.ServerInfo.MasterReplicationOffset)
	return []string{buildResponse(
		fmt.Sprintf("%s:%s", INFO_ROLE, ch.ServerInfo.Role), 
		fmt.Sprintf("%s:%s", INFO_MASTER_REPLICATION_ID, ch.ServerInfo.MasterReplicationID), 
		fmt.Sprintf("%s:%v", INFO_MASTER_REPLICATION_OFFSET, ch.ServerInfo.MasterReplicationOffset),
	)}, nil
}

func (ch *CommandsHandler) ReplConfHandler() ([]string, error) {
	return []string{"+OK\r\n"}, nil
}

func (ch *CommandsHandler) PsyncHandler() ([]string, error) {
	rdb, err := ch.Store.ToRDBStore()
	if err != nil {
		return nil, fmt.Errorf("error getting raw rdb store: %s", err.Error())
	}
	
	return []string{
		fmt.Sprintf("+FULLRESYNC %s 0\r\n", ch.ServerInfo.MasterReplicationID),
		fmt.Sprintf("$%v\r\n%s", len(rdb), rdb),
	}, nil
}

func (ch *CommandsHandler) FullResyncHandler() ([]string, error) {
	return []string{"+OK\r\n"}, nil
}

func buildResponse(messages ...string) string {
	var resp string
	if len(messages) > 1 {
		resp = strings.Join(messages, "\n")
	} else {
		resp = strings.Join(messages, "")
	}

	return fmt.Sprintf("$%v\r\n%s\r\n", len(resp), resp)
}
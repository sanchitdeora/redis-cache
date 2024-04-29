package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Command string

var numReplicasAck int
var numReplicasWait int
var replicasWaitChan = make(chan bool, 1)

const (
	// Basic Redis
	PING Command = "PING"
	ECHO Command = "ECHO"
	GET Command = "GET"
	SET Command = "SET"
	PX Command = "PX"
	
	// Replication
	INFO Command = "INFO"
	REPLCONF Command = "REPLCONF"
	PSYNC Command = "PSYNC"
	WAIT Command = "WAIT"
	FULLRESYNC Command = "FULLRESYNC"
	ACK Command = "ACK"
	GETACK Command = "GETACK"
	
	// RDB Persistence
	CONFIG Command = "CONFIG"
	DIR Command = "DIR"
	DB_FILE_NAME Command = "DBFILENAME"
	KEYS Command = "KEYS"

	// info response constants
	InfoRole = "role"
	InfoMasterReplicationID = "master_replid"
	InfoMasterReplicationOffset = "master_repl_offset"
)

type Commands struct {
	ServerOpts ServerOpts
	Store Store
}

func NewCommandsHandler(serverOpts ServerOpts, storeOpts StoreOpts) Commands{
	return Commands{
		ServerOpts: serverOpts,
		Store: NewStore(storeOpts),
	}
}

func IsWriteCommand(req string) bool {
	parsedReq, err := ParseRequest(req)
	if err != nil {
		fmt.Printf("error parsing request: %s", err.Error())
		return false
	}

	// only taking the first command
	requestLines := SplitRequests(parsedReq[0])
	if len(requestLines) < 3 {
		return false
	}
	return Command(strings.ToUpper(requestLines[2])) == SET
}

func ContainsPsyncCommand(fullRequest string) bool {
	reqs, err := ParseRequest(fullRequest)
	if err != nil {
		return false
	}

	for _, req := range reqs {
		requestLines := SplitRequests(req)
		if len(requestLines) < 3 {
			continue
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
	reqs, err := ParseRequest(fullRequest)
	if err != nil {
		return nil, fmt.Errorf("error while parsing commands: %s", err.Error())
	}

	resList := make([]string, 0)
	for _, req := range reqs {
		res, err := ch.CommandsHandler(SplitRequests(req))
		if err != nil {
			return nil, fmt.Errorf("error while parsing commands: %s", err.Error())
		}

		resList = append(resList, res...)
	}
	return resList, nil
}

func (ch *Commands) CommandsHandler(requestLines []string) (resp []string, err error) {
	// exception command cases
	if len(requestLines) == 1 {
		if strings.Contains(requestLines[0], string(FULLRESYNC)) {
			return ch.FullResyncHandler()
		} else {
			return NullResponse(), fmt.Errorf("command length should be greater than 1. request received: %s", requestLines)
		}
	}

	if len(requestLines) < 3 {
		if strings.Contains(requestLines[1], "REDIS") {
			return ch.RdbFileHandler()
		} else {
			return NullResponse(), fmt.Errorf("command length should be greater than 2. request received: %s", requestLines)
		}
	}

	command := Command(strings.ToUpper(requestLines[2]))

	switch command {
		case PING:
			resp, err = ch.PingHandler()

		case ECHO:
			resp, err = ch.EchoHandler(requestLines)

		case SET:
			resp, err = ch.SetHandler(requestLines)

		case GET:
			resp, err = ch.GetHandler(requestLines)

		case INFO:
			resp, err = ch.InfoHandler(requestLines)

		case REPLCONF:
			resp, err = ch.ReplConfHandler(requestLines)

		case PSYNC:
			resp, err = ch.PsyncHandler()

		case WAIT:
			resp, err = ch.WaitHandler(requestLines)

		case FULLRESYNC:
			resp, err = ch.FullResyncHandler()

		case CONFIG:
			resp, err = ch.ConfigHandler(requestLines)

		case KEYS:
			resp, err = ch.KeysHandler(requestLines)

		default:
			return NullResponse(), fmt.Errorf("invalid command received: %s", command)
	}

	if err != nil {
		return NullResponse(), fmt.Errorf("error receive handling command: %s", err.Error())
	}

	if ch.ServerOpts.Role == RoleSlave {
		ch.ServerOpts.ReplicaOffset += int64(len(CombineRequests(requestLines, true)))
		fmt.Printf("updating replicas offset to: %v\n", ch.ServerOpts.ReplicaOffset)
	}

	return resp, err
}

// Command Handlers
func (ch *Commands) PingHandler() ([]string, error) {
	resp, err := ResponseBuilder(SimpleStringsRespType, "PONG")
	if err != nil {
		return nil, fmt.Errorf("error creating response: %s", err.Error())
	}

	// replicas should not respond to non-REPLCONF commands
	if ch.ServerOpts.Role == RoleSlave {
		return []string{}, nil
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

	if ch.ServerOpts.Role == RoleSlave {
		return []string{}, nil
	}

	// replicas should not respond to non-REPLCONF commands
	if ch.ServerOpts.Role == RoleSlave {
		return []string{}, nil
	}

	go ch.SendToReplicas(CombineRequests(requestLines, true), nil)

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
		fmt.Sprintf("%s:%s", InfoRole, ch.ServerOpts.Role), 
		fmt.Sprintf("%s:%s", InfoMasterReplicationID, ch.ServerOpts.MasterReplicationID), 
		fmt.Sprintf("%s:%v", InfoMasterReplicationOffset, ch.ServerOpts.MasterReplicationOffset),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating response: %s", err.Error())
	}
	return []string{resp}, nil
}

func (ch *Commands) ReplConfHandler(requestLines []string) ([]string, error) {
	if len(requestLines) < 7 {
		return nil, fmt.Errorf("invalid command received. REPLCONF should have more arguments: %s", requestLines)
	}

	switch Command(requestLines[4]) {
		case GETACK:
			if ch.ServerOpts.Role == RoleSlave {
				return []string{
					fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%v\r\n%s\r\n", 
						len(strconv.Itoa(int(ch.ServerOpts.ReplicaOffset))), 
						strconv.Itoa(int(ch.ServerOpts.ReplicaOffset))),
				}, nil
			}

		case ACK:
			if ch.ServerOpts.Role == RoleSlave {
				return []string{}, nil
			}

			numReplicasAck ++
			fmt.Printf("ACK!!! numACk: %v, numWait: %v\n", numReplicasAck, numReplicasWait)

			if numReplicasAck >= numReplicasWait {
				replicasWaitChan <- true
			}
		default:
			return OKResponse(), nil

	}
	return []string{}, nil
}

func (ch *Commands) PsyncHandler() ([]string, error) {
	rdb, err := ch.Store.ToRDBStore()
	if err != nil {
		return nil, fmt.Errorf("error getting raw rdb store: %s", err.Error())
	}

	return []string{
		fmt.Sprintf("+FULLRESYNC %s 0\r\n", ch.ServerOpts.MasterReplicationID),
		fmt.Sprintf("$%v\r\n%s", len(rdb), rdb),
	}, nil
}

func (ch *Commands) WaitHandler(requestLines []string) (res []string, err error) {
	if len(requestLines) < 7 {
		return nil, fmt.Errorf("invalid command received. REPLCONF should have more arguments: %s", requestLines)
	}

	numReplicasAck = 0
	countAck := 0

	numReplicasWait, err = strconv.Atoi(requestLines[4])
	if err != nil {
		return nil, fmt.Errorf("error converting number of replicas to ACK: %s, error: %s", requestLines[4], err.Error())
	}
	timeoutMs, err := strconv.Atoi(requestLines[6])
	if err != nil {
		return nil, fmt.Errorf("error converting response time: %s, error: %s", requestLines[6], err.Error())
	}

	if numReplicasWait == 0 {
		return []string{":0\r\n"}, nil
	}

	// respChan := make(chan bool, len(ch.ServerOpts.Replicas))
	go ch.SendToReplicas("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n", nil)

	timeout := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)

	defer timeout.Stop()

	chanLoop: for {
		select {
			case <- replicasWaitChan:
				countAck = numReplicasAck
				fmt.Println("got ACK from replica: ", countAck)
				break chanLoop

			case <-timeout.C:
				fmt.Println("time is out")
				if numReplicasAck == 0 {
					countAck = len(ch.ServerOpts.Replicas)
				} else {
					countAck = numReplicasAck
				}
			break chanLoop
		}
	}

	numReplicasAck = 0
	numReplicasWait = 0

	return []string{fmt.Sprintf(":%v\r\n", countAck)}, nil
}

func (ch *Commands) FullResyncHandler() ([]string, error) {
	return []string{}, nil
}

func (ch *Commands) ConfigHandler(requestLines []string) ([]string, error) {
	if len(requestLines) < 7 {
		return nil, fmt.Errorf("invalid command received. CONFIG should have more arguments: %s", requestLines)
	}

	switch Command(strings.ToUpper(requestLines[4])) {
		case GET:
			switch Command(strings.ToUpper(requestLines[6])) {
				case DIR:
					return []string{fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%v\r\n%s\r\n", len(ch.Store.Config.Dir), ch.Store.Config.Dir)}, nil

				case DB_FILE_NAME:
					return []string{fmt.Sprintf("*2\r\n$10\r\ndbfilename\r\n$%v\r\n%s\r\n", len(ch.Store.Config.DbFileName), ch.Store.Config.DbFileName)}, nil
				
				default:
					fmt.Println("skipping unknown command received with CONFIG GET. request: ", requestLines)
					return []string{}, nil
			}
	}

	return []string{}, nil
}

func (ch *Commands) KeysHandler(requestLines []string) ([]string, error) {
	if len(requestLines) < 5 {
		return nil, fmt.Errorf("invalid command received. CONFIG should have more arguments: %s", requestLines)
	}

	switch strings.ToUpper(requestLines[4]) {

		case "*":
			keySet := ch.Store.GetKeys()
			if len(keySet) == 0 {
				return []string{}, nil
			}

			resp, err := ResponseBuilder(ArraysRespType, keySet...)
			if err != nil {
				return nil, fmt.Errorf("error creating response: %s", err.Error())
			}
			return []string{resp}, nil

		default:
			val, err := ch.Store.Get(requestLines[4])
			if err != nil {
				return nil, fmt.Errorf("error getting value for the key: %s. error:", requestLines[4], err.Error())
			}

			resp, err := ResponseBuilder(BulkStringsRespType, val)
			if err != nil {
				return nil, fmt.Errorf("error creating response: %s", err.Error())
			}
			return []string{resp}, nil
	}
}

func (ch *Commands) RdbFileHandler() ([]string, error) {
	return []string{}, nil
}

func (ch *Commands) SendToReplicas(request string, respChan chan bool) error {
	fmt.Printf("send To Replicas message: %q\n", request)
	for replicaConn := range ch.ServerOpts.Replicas {
		_, err := replicaConn.Write([]byte(request))
		if err != nil {
			return fmt.Errorf("error writing commands: %s", err.Error())
		}
	}
	return nil
}
package main

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/store"
)

type Command string
type KeyType string

var numReplicasAck int
var numReplicasWait int
var replicasWaitChan = make(chan bool, 1)

var xreadBlockChan = make(chan string)


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

	// Streams
	TYPE Command = "TYPE"
	XADD Command = "XADD"
	XRANGE Command = "XRANGE"
	XREAD Command = "XREAD"
	BLOCK Command = "BLOCK"

	// info response constants
	InfoRole = "role"
	InfoMasterReplicationID = "master_replid"
	InfoMasterReplicationOffset = "master_repl_offset"
)

type Commands struct {
	ServerOpts 	ServerOpts
	Store 		store.Store
	// StreamStore store.StreamStore
}

func NewCommandsHandler(serverOpts ServerOpts, storeOpts store.StoreOpts) Commands{
	return Commands{
		ServerOpts: serverOpts,
		Store: store.NewStore(storeOpts),
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

		case TYPE:
			resp, err = ch.TypeHandler(requestLines)

		case XADD:
			resp, err = ch.XAddHandler(requestLines)

		case XRANGE:
			resp, err = ch.XRangeHandler(requestLines)

		case XREAD:
			resp, err = ch.XReadHandler(requestLines)


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
	// replicas should not respond to non-REPLCONF commands
	if ch.ServerOpts.Role == RoleSlave {
		return []string{}, nil
	}

	return []string{ResponseBuilder(SimpleStringsRespType, "PONG")}, nil
}

func (ch *Commands) EchoHandler(requestLines []string) ([]string, error) {
	if len(requestLines) < 5 {
		return nil, fmt.Errorf("invalid command received. ECHO should have one arguments: %s", requestLines)
	}

	return []string{ResponseBuilder(BulkStringsRespType, requestLines[4])}, nil
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

	if err := ch.Store.KVStore.Set(requestLines[4], requestLines[6], expiration); err != nil {
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

	val, err := ch.Store.KVStore.Get(requestLines[4]);
	if err != nil {
		return nil, fmt.Errorf("error while getting from store with key: %s %s", requestLines[4], err.Error())
	}
	if val == "" {
		return NullResponse(), nil
	}

	return []string{ResponseBuilder(BulkStringsRespType, val)}, nil
}

func (ch *Commands) InfoHandler(requestLines []string) ([]string, error) {
	return []string{ResponseBuilder(
		BulkStringsRespType,
		fmt.Sprintf("%s:%s", InfoRole, ch.ServerOpts.Role), 
		fmt.Sprintf("%s:%s", InfoMasterReplicationID, ch.ServerOpts.MasterReplicationID), 
		fmt.Sprintf("%s:%v", InfoMasterReplicationOffset, ch.ServerOpts.MasterReplicationOffset),
	)}, nil
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
	rdb, err := ch.Store.KVStore.ToRDBStore()
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
					return []string{fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%v\r\n%s\r\n", len(ch.Store.KVStore.Config.Dir), ch.Store.KVStore.Config.Dir)}, nil

				case DB_FILE_NAME:
					return []string{fmt.Sprintf("*2\r\n$10\r\ndbfilename\r\n$%v\r\n%s\r\n", len(ch.Store.KVStore.Config.DbFileName), ch.Store.KVStore.Config.DbFileName)}, nil
				
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
			keySet := ch.Store.KVStore.GetKeys()
			if len(keySet) == 0 {
				return []string{}, nil
			}

			return []string{ResponseBuilder(ArraysRespType, keySet...)}, nil

		default:
			val, err := ch.Store.KVStore.Get(requestLines[4])
			if err != nil {
				return nil, fmt.Errorf("error getting value for the key: %s. error: %s", requestLines[4], err.Error())
			}

			return []string{ResponseBuilder(BulkStringsRespType, val)}, nil
	}
}

func (ch *Commands) TypeHandler(requestLines []string) ([]string, error) {
	if len(requestLines) < 5 {
		return nil, fmt.Errorf("invalid command received. TYPE should have more arguments: %s", requestLines)
	}

	arg := requestLines[4]

	val, err := ch.Store.KVStore.Get(arg)
	if err != nil {
		return nil, fmt.Errorf("error getting value for the key: %s. error: %s", arg, err.Error())
	}
	if val == "" {
		val, err := ch.Store.StreamStore.GetStream(arg)
		if err != nil {
			return nil, fmt.Errorf("error getting value for the key: %s. error: %s", arg, err.Error())
		}
		if val == nil {
			return NoneTypeResponse(), nil
		}

		return StreamResponse(), nil

	}

	v := reflect.TypeOf(val)
	switch v.Kind() { 
	case reflect.String:
        return StringResponse(), nil

    default:
        fmt.Printf("unexpected type %T", v)
    }

	return []string{}, nil
}

func (ch *Commands) XAddHandler(requestLines []string) ([]string, error) {
	if len(requestLines) < 11 {
		return nil, fmt.Errorf("invalid command received. XADD should have more arguments: %s", requestLines)
	}

	streamKey := requestLines[4]
	entryID := requestLines[6]

	if entryID == "0-0" {
		return []string{ResponseBuilder(ErrorsRespType, "The ID specified in XADD must be greater than 0-0")}, nil
	}

	i := 8
	entries := make([]store.StreamEntry, 0)
	for i < len(requestLines) {
		key := requestLines[i]
		value := requestLines[i+2]

		entryValue := store.StreamEntry{
			Key: key,
			Value: value,
		}
		entries = append(entries, entryValue)
		i += 4
	}

	updatedEntryId, err := ch.Store.StreamStore.SetEntry(streamKey, entryID, entries)
	if errors.Is(err, store.ErrInvalidEntryID) {
		return []string{ResponseBuilder(ErrorsRespType, "The ID specified in XADD is equal or smaller than the target stream top item")}, nil
	} else if err != nil {
		return []string{}, err
	}

	if updatedEntryId != "" {
		entryID = updatedEntryId
	}

	return []string{ResponseBuilder(BulkStringsRespType, entryID)}, nil
}

func (ch *Commands) XRangeHandler(requestLines []string) ([]string, error) {
	if len(requestLines) < 9 {
		return nil, fmt.Errorf("invalid command received. XRANGE should have more arguments: %s", requestLines)
	}

	streamValues := ch.Store.StreamStore.GetEntryRange(requestLines[4], requestLines[6], requestLines[8])
	if len(streamValues) == 0 {
		return []string{}, fmt.Errorf("stream values not found")
	}

	var resp string

	resp = fmt.Sprintf("*%v\r\n", len(streamValues))
	for _, val := range streamValues {
		resp += "*2\r\n"
		resp += ResponseBuilder(BulkStringsRespType, val.ID)

		innerResp := make([]string, 0)
		for _, entry := range val.Entry {
			innerResp = append(innerResp, entry.Key, entry.Value)
		}

		resp += ResponseBuilder(ArraysRespType, innerResp...)
	}

	return []string{resp}, nil
}

func (ch *Commands) XReadHandler(requestLines []string) ([]string, error) {
	if len(requestLines) < 9 {
		return nil, fmt.Errorf("invalid command received. XRANGE should have more arguments: %s", requestLines)
	}

	indexJ := 5
	if Command(strings.ToUpper(requestLines[4])) == BLOCK {
		indexJ += 4
		blockTimeout, err := strconv.Atoi(requestLines[6])
		if err != nil {
			return ch.internalXReadHandler(ch.GetXReadStreamsAndArrays(requestLines[indexJ:], false))
		}
		
		streamKeys, entryIDs := ch.GetXReadStreamsAndArrays(requestLines[indexJ:], true)
		go ch.XReadWithBlock(blockTimeout, streamKeys, entryIDs)

		for {
			select {
				case resp := <- xreadBlockChan:
					return []string{resp}, nil

				case <-time.After(time.Duration(70) * time.Second):
					return NullResponse(), nil
			}
		}

	} else {
		return ch.internalXReadHandler(ch.GetXReadStreamsAndArrays(requestLines[indexJ:], false))
	}

	// return []string{"should not be here"}, nil
}

func (ch *Commands) XReadWithBlock(timeout int, streamKeys, entryIDs []string) {
	if timeout > 0 {
		time.Sleep(time.Duration(timeout) * time.Millisecond)

		response, err := ch.internalXReadHandler(streamKeys, entryIDs)
		if err != nil {
			xreadBlockChan <- NullResponse()[0]
		} else {
			xreadBlockChan <- response[0]
		}
	} else {
		for {
			result, _ := ch.internalXReadHandler(streamKeys, entryIDs)
			if result != nil && result[0] != NullResponse()[0] {
				xreadBlockChan <- result[0]
				break
			}
			time.Sleep(time.Duration(10) * time.Millisecond)
		}
	}

}

func (ch *Commands) internalXReadHandler(streamKeys []string, entryIDs []string) ([]string, error) {

	readStreams := make(map[string][]store.StreamValues)
	for i := 0; i < len(streamKeys); i++ {
		readStreams[streamKeys[i]] = ch.Store.StreamStore.ReadEntry(streamKeys[i], entryIDs[i])
	}

	if len(readStreams) == 0 || (len(readStreams) == 1 && len(readStreams[streamKeys[0]]) == 0){
		return NullResponse(), nil
	}

	var resp string
	resp = fmt.Sprintf("*%v\r\n", len(readStreams))

	for streamName, streamValues := range readStreams {

		resp += "*2\r\n"
		resp += ResponseBuilder(BulkStringsRespType, streamName)

		resp += fmt.Sprintf("*%v\r\n", len(streamValues))
		for _, val := range streamValues {
			resp += "*2\r\n"
			resp += ResponseBuilder(BulkStringsRespType, val.ID)
	
			innerResp := make([]string, 0)
			for _, entry := range val.Entry {
				innerResp = append(innerResp, entry.Key, entry.Value)
			}
	
			resp += ResponseBuilder(ArraysRespType, innerResp...)
		}
	}

	return []string{resp}, nil
}

func (ch *Commands) GetXReadStreamsAndArrays(request []string, isBlock  bool) (streamKeys []string, entryIDs []string) {

	indexJ := 0
	xreadStreamCount := len(request[indexJ:]) / 4

	for i := 0; i < xreadStreamCount; i++ {
		streamKeys = append(streamKeys, request[indexJ + 1])
		indexJ += 2
	}

	if isBlock && request[len(request)-1] == "$" {
		entryIDs = make([]string, 0)
		for i := 0; i < xreadStreamCount; i++ {
			entryIDs = append(entryIDs, ch.Store.StreamStore.GetTopItemEntryID(streamKeys[i]))
		}
	} else {
		for i := 0; i < xreadStreamCount; i++ {
			entryIDs = append(entryIDs, request[indexJ + 1])
			indexJ += 2
		}
	}

	return streamKeys, entryIDs
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
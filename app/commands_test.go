package main

import (
	// "flag"
	"fmt"
	"testing"
	"time"

	"github.com/codecrafters-io/redis-starter-go/store"
	"github.com/stretchr/testify/assert"
)

// var portPtr *string = flag.String(FLAG_PORT, DEFAULT_LISTENER_PORT, FLAG_PORT_USAGE)

const (
	TEST_REPLICATION_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
)

func createCommandsHandler(role Role) Commands{
	return NewCommandsHandler(
		ServerOpts{
			ListnerPort: DefaultListenerPort,
			Role: role,
			MasterReplicationID: TEST_REPLICATION_ID,
			MasterReplicationOffset: 0,
		},
		store.StoreOpts{
			Config: store.RDBConfig{
				Dir: "./",
				DbFileName: "orange.rdb",
			},
		},
	)
}

// TestParseCommands
func TestParseCommands_Ping(t *testing.T) {
	handler := createCommandsHandler(RoleMaster)

	buf := []byte("*1\r\n$4\r\nping\r\n")	
	val, err := handler.ParseCommands(string(buf))
	assert.Nil(t, err)
	assert.Equal(t, []string{"+PONG\r\n"}, val)
}

func TestParseCommands_Echo(t *testing.T) {
	handler := createCommandsHandler(RoleMaster)

	buf := []byte("*2\r\n$4\r\necho\r\n$11\r\nHello World\r\n")	
	val, err := handler.ParseCommands(string(buf))
	assert.Nil(t, err)
	assert.Equal(t, []string{"$11\r\nHello World\r\n"}, val)
}

func TestParseCommands_Set(t *testing.T) {
	handler := createCommandsHandler(RoleMaster)

	buf := []byte("*3\r\n$3\r\nset\r\n$10\r\nstrawberry\r\n$9\r\nraspberry\r\n")	
	val, err := handler.ParseCommands(string(buf))
	assert.Nil(t, err)
	assert.Equal(t, []string{"+OK\r\n"}, val)
}

func TestParseCommands_SetWithExpiration(t *testing.T) {
	handler := createCommandsHandler(RoleMaster)

	buf := []byte("*5\r\n$3\r\nset\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n")	
	val, err := handler.ParseCommands(string(buf))
	assert.Nil(t, err)
	assert.Equal(t, []string{"+OK\r\n"}, val)
}

func TestParseCommands_SlaveReceiveMultipleSetsWithExpiration_SendsNoResponse(t *testing.T) {
	handler := createCommandsHandler(RoleSlave)

	buf := []byte("*5\r\n$3\r\nset\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n*5\r\n$3\r\nset\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n*5\r\n$3\r\nset\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n")	
	val, err := handler.ParseCommands(string(buf))
	assert.Nil(t, err)
	assert.Equal(t, []string{}, val)
}

func TestParseCommands_Get(t *testing.T) {
	handler := createCommandsHandler(RoleMaster)

	// set foo bar with 1sec expiration
	buf := []byte("*5\r\n$3\r\nset\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n")	
	handler.ParseCommands(string(buf))


	buf = []byte("*2\r\n$3\r\nget\r\n$5\r\nmango\r\n")	
	val, err := handler.ParseCommands(string(buf))
	assert.Nil(t, err)
	assert.Equal(t, []string{"$9\r\nraspberry\r\n"}, val)

	time.Sleep(500 * time.Millisecond)

	buf = []byte("*2\r\n$3\r\nget\r\n$5\r\nmango\r\n")	
	val, err = handler.ParseCommands(string(buf))
	assert.Nil(t, err)
	assert.Equal(t, []string{"$-1\r\n"}, val)
}

func TestParseCommands_Info(t *testing.T) {
	handler := createCommandsHandler(RoleMaster)

	buf := []byte("*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n")	
	val, err := handler.ParseCommands(string(buf))
	assert.Nil(t, err)
	assert.Equal(t, []string{"$87\r\nrole:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\r\n"}, val)
}

func TestParseCommands_ReplConf(t *testing.T) {
	handler := createCommandsHandler(RoleMaster)

	buf := []byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n")	
	val, err := handler.ParseCommands(string(buf))
	assert.Nil(t, err)
	assert.Equal(t, []string{"+OK\r\n"}, val)
}

func TestParseCommands_PSync(t *testing.T) {
	handler := createCommandsHandler(RoleMaster)

	buf := []byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")	
	val, err := handler.ParseCommands(string(buf))
	assert.Nil(t, err)
	assert.Equal(t, []string{
		fmt.Sprintf("+FULLRESYNC %s 0\r\n", handler.ServerOpts.MasterReplicationID),
		"$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\b\xbce\xfa\bused-mem°\xc4\x10\x00\xfa\baof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2",
	}, val)
}

func TestParseCommands_Config(t *testing.T) {
	handler := createCommandsHandler(RoleMaster)

	buf := []byte("*3\r\n$6\r\nCONFIG\r\n$3\r\nget\r\n$3\r\ndir\r\n")
	val, err := handler.ParseCommands(string(buf))
	assert.Nil(t, err)
	assert.Equal(t, []string{fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%v\r\n%s\r\n", len(handler.Store.KVStore.Config.Dir), handler.Store.KVStore.Config.Dir)}, val)
}

func TestParseCommands_Type(t *testing.T) {
	handler := createCommandsHandler(RoleMaster)

	// set foo bar with 1sec expiration
	buf := []byte("*5\r\n$3\r\nset\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n")	
	handler.ParseCommands(string(buf))

	buf = []byte("*2\r\n$4\r\nTYPE\r\n$5\r\nmango\r\n")
	val, err := handler.ParseCommands(string(buf))
	assert.Nil(t, err)
	assert.Equal(t, []string{"+string\r\n"}, val)

	buf = []byte("*2\r\n$4\r\nTYPE\r\n$6\r\norange\r\n")
	val, err = handler.ParseCommands(string(buf))
	assert.Nil(t, err)
	assert.Equal(t, []string{"+none\r\n"}, val)

	// Set Stream
	buf = []byte("*5\r\n$4\r\nxadd\r\n$6\r\norange\r\n$3\r\n0-1\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")	
	handler.ParseCommands(string(buf))

	buf = []byte("*2\r\n$4\r\nTYPE\r\n$6\r\norange\r\n")
	val, err = handler.ParseCommands(string(buf))
	assert.Nil(t, err)
	assert.Equal(t, []string{"+stream\r\n"}, val)

}

func TestParseCommands_XAdd(t *testing.T) {
	handler := createCommandsHandler(RoleMaster)

	{
		buf := []byte("*5\r\n$4\r\nxadd\r\n$6\r\norange\r\n$3\r\n0-1\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")	

		_, exists := handler.Store.StreamStore.DataStore["orange"]
		assert.False(t, exists)

		val, err := handler.ParseCommands(string(buf))
		assert.Nil(t, err)
		assert.Equal(t, []string{"$3\r\n0-1\r\n"}, val)

		streamVal, exists := handler.Store.StreamStore.DataStore["orange"]
		assert.True(t, exists)
		assert.Equal(t, "0-1", streamVal[0].ID)
		assert.Equal(t, "foo", streamVal[0].Entry[0].Key)
		assert.Equal(t, "bar", streamVal[0].Entry[0].Value)
	}

	{
		buf := []byte("*5\r\n$4\r\nxadd\r\n$10\r\nstrawberry\r\n$3\r\n0-*\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")	

		_, exists := handler.Store.StreamStore.DataStore["strawberry"]
		assert.False(t, exists)

		val, err := handler.ParseCommands(string(buf))
		assert.Nil(t, err)
		assert.Equal(t, []string{"$3\r\n0-1\r\n"}, val)

		streamVal, exists := handler.Store.StreamStore.DataStore["strawberry"]
		assert.True(t, exists)
		assert.Equal(t, "0-1", streamVal[0].ID)
		assert.Equal(t, "foo", streamVal[0].Entry[0].Key)
		assert.Equal(t, "bar", streamVal[0].Entry[0].Value)
	}

	{
		buf := []byte("*5\r\n$4\r\nxadd\r\n$10\r\nstrawberry\r\n$3\r\n1-*\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")	

		val, err := handler.ParseCommands(string(buf))
		assert.Nil(t, err)
		assert.Equal(t, []string{"$3\r\n1-0\r\n"}, val)

		streamVal, exists := handler.Store.StreamStore.DataStore["strawberry"]
		assert.True(t, exists)
		assert.Equal(t, "1-0", streamVal[1].ID)
		assert.Equal(t, "foo", streamVal[0].Entry[0].Key)
		assert.Equal(t, "bar", streamVal[0].Entry[0].Value)
	}

}

func TestParseCommands_XRange(t *testing.T) {
	handler := createCommandsHandler(RoleMaster)

	{
		buf := []byte("*5\r\n$4\r\nxadd\r\n$10\r\nstrawberry\r\n$3\r\n0-1\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")	
		handler.ParseCommands(string(buf))

		buf = []byte("*5\r\n$4\r\nxadd\r\n$10\r\nstrawberry\r\n$3\r\n0-2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")	
		handler.ParseCommands(string(buf))

		buf = []byte("*5\r\n$4\r\nxadd\r\n$10\r\nstrawberry\r\n$3\r\n0-3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")	
		handler.ParseCommands(string(buf))

		buf = []byte("*5\r\n$4\r\nxadd\r\n$10\r\nstrawberry\r\n$3\r\n0-4\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")	
		handler.ParseCommands(string(buf))
	
		buf = []byte("*4\r\n$6\r\nxrange\r\n$10\r\nstrawberry\r\n$1\r\n0\r\n$3\r\n0-2\r\n")	
		val, err := handler.ParseCommands(string(buf))
		assert.Nil(t, err)
		assert.Equal(t, []string{"*2\r\n*2\r\n$3\r\n0-1\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"}, val)

		buf = []byte("*4\r\n$6\r\nxrange\r\n$10\r\nstrawberry\r\n$1\r\n-\r\n$3\r\n0-2\r\n")	
		val, err = handler.ParseCommands(string(buf))
		assert.Nil(t, err)
		assert.Equal(t, []string{"*2\r\n*2\r\n$3\r\n0-1\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"}, val)

		buf = []byte("*4\r\n$6\r\nxrange\r\n$10\r\nstrawberry\r\n$1\r\n0\r\n$1\r\n+\r\n")	
		val, err = handler.ParseCommands(string(buf))
		assert.Nil(t, err)
		assert.Equal(t, []string{"*4\r\n*2\r\n$3\r\n0-1\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\n0-4\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"}, val)

	}

}

func TestIsWriteCommand(t *testing.T) {
	isWrite := IsWriteCommand("*1\r\n$4\r\nping\r\n")
	assert.False(t, isWrite)

	isWrite = IsWriteCommand("*3\r\n$3\r\nset\r\n$3\r\nbaz\r\n$3\r\n789\r\n")
	assert.True(t, isWrite)
}

func TestIsPsyncCommand(t *testing.T) {
	isWrite := ContainsPsyncCommand("*1\r\n$4\r\nping\r\n")
	assert.False(t, isWrite)

	isWrite = ContainsPsyncCommand("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
	assert.True(t, isWrite)

	isWrite = ContainsPsyncCommand("*1\r\n$4\r\nping\r\n*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
	assert.True(t, isWrite)

	isWrite = ContainsPsyncCommand("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
	assert.True(t, isWrite)
}

func TestReadFromRDBFile(t *testing.T) {
	{
		handler := createCommandsHandler(RoleMaster)
		handler.Store.KVStore.Config.DbFileName = "EmptyRDBTest"

		handler.Store.KVStore.InitializeDB()
		assert.Equal(t, 0, len(handler.Store.KVStore.DataStore))
	}

	{
		handler := createCommandsHandler(RoleMaster)
		handler.Store.KVStore.Config.DbFileName = "RDBTest"

		handler.Store.KVStore.InitializeDB()
		assert.Equal(t, 1, len(handler.Store.KVStore.DataStore))
	}
}

func TestResponseBuilder(t *testing.T) {
	{
		val := ResponseBuilder(SimpleStringsRespType, "FULLRESYNC 0 xxxxx")
		assert.Equal(t, "+FULLRESYNC 0 xxxxx\r\n", val)

		val = ResponseBuilder(SimpleStringsRespType, "FULLRESYNC 0 xxxxx", "testing")
		assert.Equal(t, "", val)
	}

	{
		val := ResponseBuilder(BulkStringsRespType, "pear")
		assert.Equal(t, "$4\r\npear\r\n", val)

		val = ResponseBuilder(BulkStringsRespType, "pear", "banana")
		assert.Equal(t, "$11\r\npear\nbanana\r\n", val)
	}

	{
		val := ResponseBuilder(ArraysRespType, "pear")
		assert.Equal(t, "*1\r\n$4\r\npear\r\n", val)

		val = ResponseBuilder(ArraysRespType, "pear", "banana")
		assert.Equal(t, "*2\r\n$4\r\npear\r\n$6\r\nbanana\r\n", val)
	}

	{
		val := ResponseBuilder(ErrorsRespType, "pear")
		assert.Equal(t, "-ERR pear\r\n", val)

		val = ResponseBuilder(ErrorsRespType, "pear", "banana")
		assert.Equal(t, "", val)
	}
}


// TestParseRequest

func TestParseRequest(t *testing.T) {
	req, err := ParseRequest("*1\r\n$4\r\nping\r\n")
	assert.Nil(t, err)
	assert.Equal(t, []string{"*1\r\n$4\r\nping\r\n"}, req)

	_, err = ParseRequest("1\r\n$4\r\nping\r\n")
	assert.NotNil(t, err)

	req, err = ParseRequest("*2\r\n$4\r\necho\r\n$11\r\nHello World\r\n")
	assert.Nil(t, err)
	assert.Equal(t, []string{"*2\r\n$4\r\necho\r\n$11\r\nHello World\r\n"}, req)

	req, err = ParseRequest("+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n")
	assert.Nil(t, err)
	assert.Equal(t, []string{"+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n"}, req)

	req, err = ParseRequest("$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\b\xbce\xfa\bused-mem°\xc4\x10\x00\xfa\baof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2")
	assert.Nil(t, err)
	assert.Equal(t, []string{"$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\b\xbce\xfa\bused-mem°\xc4\x10\x00\xfa\baof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2"}, req)

	req, err = ParseRequest("+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\b\xbce\xfa\bused-mem°\xc4\x10\x00\xfa\baof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")
	assert.Nil(t, err)
	assert.Equal(t, []string{
		"+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n",
		"$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\b\xbce\xfa\bused-mem°\xc4\x10\x00\xfa\baof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2",
		"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n",
	}, req)

	req, err = ParseRequest("*5\r\n$3\r\nset\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n*5\r\n$3\r\nset\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n*5\r\n$3\r\nset\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n")
	assert.Nil(t, err)
	assert.Equal(t, []string{
		"*5\r\n$3\r\nset\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n",
		"*5\r\n$3\r\nset\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n",
		"*5\r\n$3\r\nset\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n",
	}, req)
}

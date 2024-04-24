package main

import (
	// "flag"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// var portPtr *string = flag.String(FLAG_PORT, DEFAULT_LISTENER_PORT, FLAG_PORT_USAGE)

const (
	TEST_REPLICATION_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
)

func createCommandsHandler(role Role) CommandsHandler{
	return NewCommandsHandler(
		CommandOpts{
			ServerInfo: ServerOpts{
				ListnerPort: DefaultListenerPort,
				Role: role,
				MasterReplicationID: TEST_REPLICATION_ID,
				MasterReplicationOffset: 0,
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
		fmt.Sprintf("+FULLRESYNC %s 0\r\n", handler.ServerInfo.MasterReplicationID),
		"$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\b\xbce\xfa\bused-mem°\xc4\x10\x00\xfa\baof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2",
	}, val)
}

// TestParseRequest

func TestParseRequest(t *testing.T) {
	req, err := ParseRequest("*1\r\n$4\r\nping\r\n")
	assert.Nil(t, err)
	assert.Equal(t, []string{"*1", "$4", "ping"}, req)

	req, err = ParseRequest("*2\r\n$4\r\necho\r\n$11\r\nHello World\r\n")
	assert.Nil(t, err)
	assert.Equal(t, []string{"*2", "$4", "echo", "$11", "Hello World"}, req)

	req, err = ParseRequest("*5\r\n$3\r\nset\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n")
	assert.Nil(t, err)
	assert.Equal(t, []string{"*5", "$3", "set", "$5", "mango", "$9", "raspberry", "$2", "px", "$3", "100"}, req)
}
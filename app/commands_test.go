package main

import (
	// "flag"
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
				ListnerPort: DEFAULT_LISTENER_PORT,
				Role: role,
				ReplicationID: TEST_REPLICATION_ID,
				ReplicationOffset: 0,
			},
		},
	)
}

// TestParseCommands
func TestParseCommands_Ping(t *testing.T) {
	handler := createCommandsHandler(ROLE_MASTER)

	buff := []byte("*1\r\n$4\r\nping\r\n")	
	val, err := handler.ParseCommands(buff, len(buff))
	assert.Nil(t, err)
	assert.Equal(t, "+PONG\r\n", val)
}

func TestParseCommands_Echo(t *testing.T) {
	handler := createCommandsHandler(ROLE_MASTER)

	buff := []byte("*1\r\n$4\r\necho\r\n$11\r\nHello World\r\n")	
	val, err := handler.ParseCommands(buff, len(buff))
	assert.Nil(t, err)
	assert.Equal(t, "$11\r\nHello World\r\n", val)
}

func TestParseCommands_Set(t *testing.T) {
	handler := createCommandsHandler(ROLE_MASTER)

	buff := []byte("*1\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\bar\r\n")	
	val, err := handler.ParseCommands(buff, len(buff))
	assert.Nil(t, err)
	assert.Equal(t, "+OK\r\n", val)
}

func TestParseCommands_SetWithExpiration(t *testing.T) {
	handler := createCommandsHandler(ROLE_MASTER)

	buff := []byte("*5\r\n$3\r\nset\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n")	
	val, err := handler.ParseCommands(buff, len(buff))
	assert.Nil(t, err)
	assert.Equal(t, "+OK\r\n", val)
}

func TestParseCommands_Get(t *testing.T) {
	handler := createCommandsHandler(ROLE_MASTER)

	// set foo bar with 1sec expiration
	buff := []byte("*5\r\n$3\r\nset\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n")	
	handler.ParseCommands(buff, len(buff))


	buff = []byte("*1\r\n$3\r\nget\r\n$5\r\nmango")	
	val, err := handler.ParseCommands(buff, len(buff))
	assert.Nil(t, err)
	assert.Equal(t, "$9\r\nraspberry\r\n", val)

	time.Sleep(500 * time.Millisecond)

	buff = []byte("*1\r\n$3\r\nget\r\n$5\r\nmango")	
	val, err = handler.ParseCommands(buff, len(buff))
	assert.Nil(t, err)
	assert.Equal(t, "$-1\r\n", val)
}

func TestParseCommands_Info(t *testing.T) {
	handler := createCommandsHandler(ROLE_MASTER)

	buff := []byte("*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n")	
	val, err := handler.ParseCommands(buff, len(buff))
	assert.Nil(t, err)
	assert.Equal(t, "$87\r\nrole:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\r\n", val)
}
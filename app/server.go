package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/store"
)

type Role string

const (
	DefaultListenerPort = "6379"
	DefaultBufferSize = 4096

	// flag constants
	FlagPort = "port"
	FlagPortUsage = "port to listen on"

	FlagReplicaOf = "replicaof"
	FlagReplicaOfUsage = "server role"

	FlagDir = "dir"
	FlagDirUsage = "Provide directory location"

	FlagDBFileName = "dbfilename"
	FlagDBFileNameUsage = "Provides DB File Name"

	// server constants
	TcpNetwork = "tcp"
	ReplicaIdLength = 40

	// role constants
	RoleMaster 	Role = "master"
	RoleSlave 	Role = "slave"
)

type ServerOpts struct {
	ListnerPort 			string
	Role 					Role
	MasterReplicationID 	string
	MasterReplicationOffset int64

	MasterHost 				string
	MasterPort 				string

	ReplicaOffset 			int64

	Replicas 				map[net.Conn]int
}

type Server struct {
	ServerOpts
	listner  	net.Listener
	commands  	Commands

	MasterConn 	net.Conn
}

// NewServer() Creates a new Server
func NewServer(serverOpts ServerOpts, storeOpts store.StoreOpts) Server {
	return Server{
		ServerOpts: serverOpts,
		commands: NewCommandsHandler(serverOpts, storeOpts),
	}
}

func main() {
	portPtr := flag.String(FlagPort, DefaultListenerPort, FlagPortUsage)
	replicaOfPtr := flag.String(FlagReplicaOf, "", FlagReplicaOfUsage)

	dirPtr := flag.String(FlagDir, ".", "--dir")
	dbFileNamePtr := flag.String(FlagDBFileName, "dump.rdb", "--dbfilename")

	flag.Parse()

	serverOpts := ServerOpts{
		ListnerPort: *portPtr,
		Replicas: make(map[net.Conn]int, 0),
	}

	if len(*replicaOfPtr) > 0 {
		// Replica Props
		serverOpts.Role = RoleSlave
		serverOpts.MasterHost = *replicaOfPtr
		serverOpts.MasterPort = flag.Arg(0)
		serverOpts.MasterReplicationOffset = -1
		serverOpts.ReplicaOffset = 0
	
	} else {
		// master Props
		serverOpts.Role = RoleMaster
		serverOpts.MasterReplicationID = GenerateAlphaNumericString(ReplicaIdLength)
		serverOpts.MasterReplicationOffset = 0
		serverOpts.ReplicaOffset = -1
	}

	storeOpts := store.StoreOpts{
		Config: store.RDBConfig{
			Dir: *dirPtr,
			DbFileName: *dbFileNamePtr,
		},
	}

	server := NewServer(serverOpts, storeOpts)

	if server.Role == RoleSlave {
		server.handshakeMaster()
		go server.handleConn(server.MasterConn)
	}

	server.commands.Store.KVStore.InitializeDB()
	server.StartServer()
}

func (s *Server) StartServer() {
	l, err := net.Listen(TcpNetwork, fmt.Sprintf(":%s", s.ListnerPort))
	if err != nil {
		fmt.Println("Failed to bind to port", s.ListnerPort)
		os.Exit(1)
	}

	defer l.Close()

	s.listner = l
	s.Run()
}

func (s *Server) Run() {
	for {
		conn, err := s.listner.Accept()
		if err != nil {
			fmt.Println("error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		buf := make([]byte, DefaultBufferSize)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("error reading from main connection: ", err.Error())
			return
		}
		if n == 0 {
			fmt.Println("buffer is empty")
			return
		}

		fmt.Printf("Message Received: %q\n", buf[:n])

		// parse requests
		req := string(buf[:n])
		
		err = s.HandleRequests(conn, req)
		if err != nil {
			fmt.Printf("error processing request: %s", err.Error())
			return
		}
	}
}

func (s *Server) HandleRequests(conn net.Conn, req string) error {
	// parse requests
	responses, err := s.commands.ParseCommands(req)
	if err != nil {
		return fmt.Errorf("error parsing commands: %s", err.Error())
	}

	// store replicas
	if ContainsPsyncCommand(req) {
		_, ok := s.Replicas[conn]
		if !ok {
			s.Replicas[conn] = 0
		}
	}

	// write responses
	err = s.writeMessages(conn, responses)
	if err != nil {
		return fmt.Errorf("error writing messages: %s", err.Error())
	}

	return nil
}


func (s *Server) writeMessages(conn net.Conn, messages []string) error {
	if len(messages) == 0 {
		return nil
	}

	fmt.Printf("writing messages to conn: %q\n", messages)
	for _, message := range messages {
		_, err := conn.Write([]byte(message))
		if err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	}
	return nil
}

func (s *Server) handshakeMaster() {
	conn, err := net.Dial(TcpNetwork, fmt.Sprintf("%s:%s", s.MasterHost, s.MasterPort))
	if err != nil {
		fmt.Printf("Failed to bind to master host: %s port:%s error:%s\n", s.MasterHost, s.MasterPort, err.Error())
		return
	}

	s.MasterConn = conn

	// start handshake
	// Send PING to master
	_, err = conn.Write([]byte("*1\r\n$4\r\nping\r\n"))
	if err != nil {
		fmt.Println("error writing to connection: ", err.Error())
		return
	}

	_, err = conn.Read(make([]byte, 1024))
	if err != nil {
		return
	}

	// Send first REPLCONF to master with slave listening PORT
	_, err = conn.Write([]byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%v\r\n%s\r\n", len(s.ListnerPort), s.ListnerPort)))
	if err != nil {
		fmt.Println("error writing to connection: ", err.Error())
		return
	}

	_, err = conn.Read(make([]byte, DefaultBufferSize))
	if err != nil {
		fmt.Println("error reading from connection: ", err.Error())
		return
	}

	// Send second REPLCONF to master with PSYNC2 Capability
	_, err = conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	if err != nil {
		fmt.Println("error writing to connection: ", err.Error())
		return
	}

	buf := make([]byte, DefaultBufferSize)
	_, err = conn.Read(buf)
	if err != nil {
		fmt.Println("error reading from connection: ", err.Error())
		return
	}

	// Send first PSYNC to master with PSYNC2 Capability
	sendReplicationID := s.MasterReplicationID
	if len(s.MasterReplicationID) == 0 {
		sendReplicationID = "?"
	}
	sendOffset := strconv.Itoa(int(s.MasterReplicationOffset))

	_, err = conn.Write([]byte(fmt.Sprintf("*3\r\n$5\r\nPSYNC\r\n$%v\r\n%s\r\n$%v\r\n%s\r\n", len(sendReplicationID), sendReplicationID, len(sendOffset), sendOffset)))
	if err != nil {
		fmt.Println("error writing to connection: ", err.Error())
		return
	}

	// n, err := conn.Read(buf)
	// if err != nil {
	// 	fmt.Println("error reading from connection: ", err.Error())
	// 	return
	// } 
}
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	// "strings"
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

	// server constants
	TcpNetwork = "tcp"
	ReplicaIdLength = 40

	// role constants
	RoleMaster Role = "master"
	RoleSlave Role = "slave"
)

type ServerOpts struct {
	ListnerPort string
	Role Role
	MasterReplicationID string
	MasterReplicationOffset int64

	MasterHost string
	MasterPort string

	ReplicaOffset int64
}

type Server struct {
	ServerOpts
	listner  net.Listener
	commands  Commands

	MasterConn net.Conn
	Replicas map[net.Conn]int
}

// NewServer() Creates a new Server
func NewServer(opts ServerOpts) Server {
	return Server{
		ServerOpts: opts,
		commands: NewCommandsHandler(
			CommandOpts{
				ServerOpts: opts,
			},
		),
		Replicas: make(map[net.Conn]int, 0),
	}
}

func main() {
	portPtr := flag.String(FlagPort, DefaultListenerPort, FlagPortUsage)
	replicaOfPtr := flag.String(FlagReplicaOf, "", FlagReplicaOfUsage)
	flag.Parse()

	opts := ServerOpts{
		ListnerPort: *portPtr,
	}

	if len(*replicaOfPtr) > 0 {
		// Replica Props
		opts.Role = RoleSlave
		opts.MasterHost = *replicaOfPtr
		opts.MasterPort = flag.Arg(0)
		opts.MasterReplicationOffset = -1
		opts.ReplicaOffset = 0
	
	} else {
		// master Props
		opts.Role = RoleMaster
		opts.MasterReplicationID = GenerateAlphaNumericString(ReplicaIdLength)
		opts.MasterReplicationOffset = 0
		opts.ReplicaOffset = -1
	}

	server := NewServer(opts)

	if server.Role == RoleSlave {
		server.handshakeMaster()
		go server.handleConn(server.MasterConn)
	}

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
			fmt.Println("error reading from connection: ", err.Error())
			return
		}
		if n == 0 {
			fmt.Println("buffer is empty")
			return
		}

		fmt.Printf("Message Received: %s", buf[:n])

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

	// send write commands to replicas
	err = s.sendWriteCommandsToReplicas(req)
	if err != nil {
		return fmt.Errorf("error sending writing commands to replicas: %s", err.Error())
	}

	return nil
}

func (s *Server) sendWriteCommandsToReplicas(fullRequest string) error {
	reqs, err := ParseRequest(fullRequest)
	if err != nil {
		return fmt.Errorf("error parsing commands: %s", err.Error())
	}

	for _, req := range reqs {
		fmt.Printf("---DEBUG---\nRequest: %s, Role: %s, IsWriteCommand: %v\n", req, s.Role, IsWriteCommand(req))
		if s.Role == RoleMaster && IsWriteCommand(req) {
			fmt.Printf("---DEBUG---\nList of replicas length: %v\n", len(s.Replicas))
			for replica, offset := range s.Replicas {
				fmt.Printf("---DEBUG---\nReplica Host: %s, Offset: %v\n", replica.LocalAddr(), offset)
				replica.Write([]byte(req))
			}
		}
	}

	return nil
}

func (s *Server) writeMessages(conn net.Conn, messages []string) error {
	if len(messages) == 0 {
		return nil
	}

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
		fmt.Printf("Failed to bind to master host: %s port:%s error:%s", s.MasterHost, s.MasterPort, err.Error())
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
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
)

type Role string

const (
	DefaultListenerPort = "6379"
	
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
}

type Server struct {
	ServerOpts
	listner  net.Listener
	handler  CommandsHandler
	replicas []net.Conn
}

// NewServer() Creates a new Server
func NewServer(opts ServerOpts) Server {
	return Server{
		ServerOpts: opts,
		handler: NewCommandsHandler(
			CommandOpts{
				ServerInfo: opts,
			},
		),
		replicas: make([]net.Conn, 0),
	}
}

func main() {
	portPtr := flag.String(FlagPort, DefaultListenerPort, FlagPortUsage)
	replicaOfPtr := flag.String(FlagReplicaOf, "", FlagReplicaOfUsage)
	flag.Parse()

	var serverRole Role
	var masterHost string
	var masterPort string
	var replicationId string
	var offset int64
	
	if len(*replicaOfPtr) > 0 {
		serverRole = RoleSlave
		masterHost = *replicaOfPtr
		masterPort = flag.Arg(0)
		offset = -1
	} else {
		serverRole = RoleMaster
		replicationId = GenerateAlphaNumericString(ReplicaIdLength)
		offset = 0
	}

	opts := ServerOpts{
		ListnerPort: *portPtr,
		Role: Role(serverRole),
		MasterReplicationID: replicationId,
		MasterReplicationOffset: offset,
	}
	server := NewServer(opts)

	if server.Role == RoleSlave {
		server.handshakeMaster(fmt.Sprintf("%s:%s", masterHost, masterPort))
	}

	server.StartServer()
}

func (s *Server) handshakeMaster(masterAddr string) {
	conn, err := net.Dial(TcpNetwork, masterAddr)
	if err != nil {
		fmt.Println("Failed to bind to port:", masterAddr, err)
		return
	}

	defer conn.Close()

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

	_, err = conn.Read(make([]byte, 1024))
	if err != nil {
		return
	}

	// Send second REPLCONF to master with PSYNC2 Capability
	_, err = conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	if err != nil {
		fmt.Println("error writing to connection: ", err.Error())
		return
	}

	_, err = conn.Read(make([]byte, 1024))
	if err != nil {
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

	_, err = conn.Read(make([]byte, 1024))
	if err != nil {
		return
	}
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

		go s.handleConn(conn, s.handler)
	}
}

func (s *Server) handleConn(conn net.Conn, handler CommandsHandler) {
	defer conn.Close()

	buf := make([]byte, 128)
	for {
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

		// responses
		req := string(buf[:n])
		responses, err := handler.ParseCommands(req)
		if err != nil {
			fmt.Println("error parsing commands: ", err.Error())
			return
		}

		// store replicas
		if IsPsyncCommand(req) {
			s.replicas = append(s.replicas, conn)
		}

		// write responses
		s.writeMessages(conn, responses)

		// send write commands request to replicas
		if s.Role == RoleMaster && IsWriteCommand(req) {
			fmt.Printf("Role: %s, Replicas List: %v\n", s.Role, len(s.replicas))

			for i, replica := range s.replicas {
				fmt.Printf("Replica %v: LocalAddr: %s RemoteAddr: %s Other: %s\r\n", i, replica.LocalAddr(), replica.RemoteAddr(), replica)
				fmt.Printf("Write to replica response: %s", buf)

				replica.Write(buf[:n])
			}
		}
	}
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

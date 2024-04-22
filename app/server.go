package main

import (
	"flag"
	"fmt"
	"net"
	"os"
)

type Role string

const (
	DEFAULT_LISTENER_PORT = "6379"
	
	// flag constants
	FLAG_PORT = "port"
	FLAG_PORT_USAGE = "port to listen on"
	
	FLAG_REPLICA_OF = "replicaof"
	FLAG_REPLICA_OF_USAGE = "server role"

	// server constants
	TCP_NETWORK = "tcp"
	REPLICA_ID_LENGTH = 40

	// role constants
	ROLE_MASTER Role = "master"
	ROLE_SLAVE Role = "slave"
)

type ServerOpts struct {
	ListnerPort string
	Role Role
	ReplicationID string
	ReplicationOffset int64
}

type Server struct {
	ServerOpts
	listner net.Listener
	handler CommandsHandler
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
	}
}

func main() {
	portPtr := flag.String(FLAG_PORT, DEFAULT_LISTENER_PORT, FLAG_PORT_USAGE)
	replicaOfPtr := flag.String(FLAG_REPLICA_OF, "", FLAG_REPLICA_OF_USAGE)
	flag.Parse()

	port := fmt.Sprintf(":%s", *portPtr)

	var serverRole Role
	var masterHost string
	var masterPort string
	var replicationId string
	
	if len(*replicaOfPtr) > 0 {
		serverRole = ROLE_SLAVE
		masterHost = *replicaOfPtr
		masterPort = flag.Arg(0)
	} else {
		serverRole = ROLE_MASTER
		replicationId = GenerateAlphaNumericString(REPLICA_ID_LENGTH)
	}

	opts := ServerOpts{
		ListnerPort: port,
		Role: Role(serverRole),
		ReplicationID: replicationId,
		ReplicationOffset: 0,
	}
	server := NewServer(opts)

	if server.Role == ROLE_SLAVE {
		handshakeMaster(fmt.Sprintf("%s:%s", masterHost, masterPort))
	}

	server.StartServer()
}

func handshakeMaster(masterAddr string) {
	conn, err := net.Dial(TCP_NETWORK, masterAddr)
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
}

func (s *Server) StartServer() {
	l, err := net.Listen(TCP_NETWORK, s.ListnerPort)
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

		go handleConn(conn, s.handler)
	}
}

func handleConn(conn net.Conn, handler CommandsHandler) {
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
	

		response, err := handler.ParseCommands(buf, n)
		fmt.Printf("--- debug Response ---\nResponse:%s\n", response)
		if err != nil {
			fmt.Println("error parsing commands: ", err.Error())
			return
		}

		_, err = conn.Write([]byte(response))
		if err != nil {
			fmt.Println("error writing to connection: ", err.Error())
			return
		}
	}
}

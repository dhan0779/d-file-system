package namenode

import (
	"log"
	"net"
	"net/rpc"
	"strconv"
)

type NameNodeService struct {
	BlockSize uint64
	ReplicationFactor uint64
	Port uint64
}

func New(blockSize uint64, replicationFactor uint64) *NameNodeService {
	return &NameNodeService{
		BlockSize: blockSize,
		ReplicationFactor: replicationFactor,
	}
}

func Initialize(port int) {
	instance := new(NameNodeService)
	instance.Port = uint64(port)

	err := rpc.Register(instance)
	if err != nil {
		panic(err)
	}

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))

	if err != nil {
		panic(err)
	}

	rpc.Accept(listener)
	log.Println("datanode started on port: " + strconv.Itoa(port))
}
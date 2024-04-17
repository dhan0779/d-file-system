package datanode

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"strconv"
)

type DataNodeService struct {
	Port uint64
	NameNodePort uint64
}

func Heartbeat(req bool, res *bool) error {
	if req {
		log.Println("received from Namenode")
		*res = true
		return nil
	}

	return errors.New("heartbeat request error")
} 

func Initialize(port int) {
	instance := new(DataNodeService)
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

func (dataNode *DataNodeService) GetData()
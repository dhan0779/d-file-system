package datanode

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"strconv"
)

type DataNodeService struct {
	Port int
}

func (dataNode *DataNodeService) Heartbeat(req bool, res *bool) error {
	log.Println(req)
	if req {
		log.Println("received from Namenode")
		*res = true
		return nil
	}

	return errors.New("heartbeat request error")
} 

func Initialize(port int) {
	instance := new(DataNodeService)
	instance.Port = int(port)

	err := rpc.Register(instance)
	if err != nil {
		panic(err)
	}

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))

	if err != nil {
		panic(err)
	}

	log.Println("datanode started on port: " + strconv.Itoa(port))
	nameNodeInstance, err := rpc.Dial("tcp", ":8000")
	if err != nil {
		panic(err)
	}

	res := false
	err = nameNodeInstance.Call("NameNodeService.AddDataNode", port, &res)
	if err != nil || !res {
		panic("Could not add data node to name node")
	}

	rpc.Accept(listener)
}

func (dataNode *DataNodeService) GetData() {}
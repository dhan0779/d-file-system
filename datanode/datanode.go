package datanode

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"strconv"
)

type Service struct {
	Port int
	Host string
}

func (dataNode *Service) Heartbeat(req bool, res *bool) error {
	log.Println("hi")
	if req {
		log.Println("received from Namenode")
		*res = true
		return nil
	}

	return errors.New("heartbeat request error")
} 

func Initialize(host string, port int, server_port int) {
	instance := new(Service)
	instance.Port = port
	instance.Host = host

	err := rpc.Register(instance)
	if err != nil {
		panic(err)
	}

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", host + ":" + strconv.Itoa(port))

	if err != nil {
		panic(err)
	}

	log.Println("datanode started on port: " + strconv.Itoa(port))
	nameNodeInstance, err := rpc.Dial("tcp", host + ":" + strconv.Itoa(server_port))
	if err != nil {
		panic(err)
	}

	res := false
	err = nameNodeInstance.Call("Service.AddDataNode", port, &res)
	if err != nil || !res {
		panic("Could not add data node to name node")
	}

	rpc.Accept(listener)
}

func (dataNode *Service) GetData() {}
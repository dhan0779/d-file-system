package datanode

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
)

type Service struct {
	Port int
	Host string
	Directory string
}

type WriteRequest struct {
	BlockId string
	BlockData []byte
}

type ReadRequest struct {
	BlockId string
	BlockSize int
}

type ReadResponse struct {
	DataBuffer []byte
}

func (dataNode *Service) Heartbeat(req bool, res *bool) error {
	if req {
		log.Println("heartbeat acknowledged")
		*res = true
		return nil
	}

	return errors.New("heartbeat request error")
} 

func New(host string, port int, directory string) *Service {
	return &Service{
		Port: port,
		Host: host,
		Directory : directory,
	}
}

func Initialize(host string, port int, server_port int) {
	storageDirectory := "./storage/" + strconv.Itoa(port) + "/"
	instance := New(host, port, storageDirectory)
	os.MkdirAll(storageDirectory, os.ModePerm)
	log.Println(storageDirectory)

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

func (dataNode *Service) WriteData(req *WriteRequest, res *bool) error {
	f, err := os.Create(dataNode.Directory + req.BlockId)
	if err != nil {
		return errors.New("Could not create blockId file")
	}

	if _, err := f.Write(req.BlockData); err != nil {
		return err
	}

	*res = true
	return nil
}

func (dataNode *Service) ReadData(req *ReadRequest, res *ReadResponse) error {
	f, err := os.Open(dataNode.Directory + req.BlockId)
	buffer := make([]byte, req.BlockSize)
	if err != nil {
		return errors.New("Could not read from blockId")
	}

	if _, err := f.Read(buffer); err != nil {
		return err
	}
	res.DataBuffer = buffer

	return nil
}
package namenode

import (
	"log"
	"net"
	"net/rpc"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type Service struct {
	BlockSize int
	ReplicationFactor int
	Port int
	Host string
	FileToBlocks map[string][]string
	BlocksToDataNodes map[string][]int // replication
	DataNodeIds map[int]struct{} // we can use ports as datanode ids and store in set
}

type WriteRequest struct {
	FileName string
	FileSize int
}

type Metadata struct {

}

func New(host string, port int, blockSize int, replicationFactor int) *Service {
	return &Service{
		BlockSize: blockSize,
		ReplicationFactor: replicationFactor,
		Port: port,
		Host: host,
		DataNodeIds: make(map[int]struct{}),
		BlocksToDataNodes: make(map[string][]int),
		FileToBlocks: make(map[string][]string),
	}
}

func Initialize(host string, port int) {
	instance := New(host, port, 64, 3)

	go instance.heartbeatRoutine() // starts heartbeat routine for datanodes

	err := rpc.Register(instance)
	if err != nil {
		panic(err)
	}

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", host + ":" + strconv.Itoa(port))

	if err != nil {
		panic(err)
	}
	log.Println("namenode started on port: " + strconv.Itoa(port))
	rpc.Accept(listener)
}

func (nameNode *Service) heartbeatRoutine() {
	for range time.Tick(time.Second) {
		dataNodePorts := make([]int, 0, len(nameNode.DataNodeIds))
		for k := range nameNode.DataNodeIds {
			dataNodePorts = append(dataNodePorts, k)
		}
		log.Println(dataNodePorts)

		for _, dataNodePort := range dataNodePorts {
			dataNodeInstance, err := rpc.Dial("tcp", nameNode.Host + ":" + strconv.Itoa(dataNodePort))

			if err != nil {
				log.Printf("No connection to datanode at port %d\n", dataNodePort)
				delete(nameNode.DataNodeIds, dataNodePort) // delete data node from namenode 
				// redistribute data here
				continue
			}

			res := false
			err = dataNodeInstance.Call("Service.Heartbeat", true, &res)
			if err != nil || !res {
				log.Println(err)
				log.Printf("No heartbeat from datanode at port %d\n", dataNodePort)
				delete(nameNode.DataNodeIds, dataNodePort) // delete data node from namenode
				// redistribute data here
			}
		}
	}
}

func (nameNode *Service) GetBlockSize(req bool, res *int) error {
	if req {
		*res = nameNode.BlockSize
	}
	return nil
}

func (nameNode *Service) AddDataNode(req int, res *bool) error {
	nameNode.DataNodeIds[req] = struct{}{}
	*res = true
	return nil
}

// func (nameNode *Service) GetMetadataFromWrite(req *WriteRequest, res *[]Metadata) error {
// 	nameNode.FileToBlocks[req.FileName] = []string{}
// 	numBlocks := int(int(req.FileSize) / int(nameNode.BlockSize))

// 	return nil
// }

func (nameNode *Service) assignNodes(fileName string, numBlocks int) []Metadata {
	metadata := []Metadata{}

	for i := 0; i < int(numBlocks); i++ {
		blockId := uuid.NewString()
		nameNode.FileToBlocks[fileName] = append(nameNode.FileToBlocks[fileName], blockId)
		nameNode.BlocksToDataNodes[blockId] = []int{}


	}
	return metadata
}


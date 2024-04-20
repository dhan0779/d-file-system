package namenode

import (
	"log"
	"net"
	"net/rpc"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type NameNodeService struct {
	BlockSize int
	ReplicationFactor int
	Port int
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

func New(blockSize int, replicationFactor int) *NameNodeService {
	return &NameNodeService{
		BlockSize: blockSize,
		ReplicationFactor: replicationFactor,
	}
}

func Initialize(port int) {
	instance := new(NameNodeService)
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
	log.Println("namenode started on port: " + strconv.Itoa(port))

	go instance.heartbeatRoutine() // starts heartbeat routine for datanodes
	rpc.Accept(listener)
}

func (nameNode *NameNodeService) heartbeatRoutine() {
	dataNodePorts := make([]int, 0, len(nameNode.DataNodeIds))
	for k := range nameNode.DataNodeIds {
		dataNodePorts = append(dataNodePorts, k)
	}
	for range time.Tick(time.Second) {
		for _, dataNodePort := range dataNodePorts {
			dataNodeInstance, err := rpc.Dial("tcp", strconv.Itoa(dataNodePort))

			if err != nil {
				log.Printf("No connection to datanode at port %d\n", dataNodePort)
				delete(nameNode.DataNodeIds, dataNodePort) // delete data node from namenode 
				// redistribute data here
				continue
			}

			var res bool
			err = dataNodeInstance.Call("DataNodeService.Heartbeat", true, &res)
			if err != nil || !res {
				log.Printf("No heartbeat from datanode at port %d\n", dataNodePort)
				delete(nameNode.DataNodeIds, dataNodePort) // delete data node from namenode
				// redistribute data here
			}

		}
	}
}

func (nameNode *NameNodeService) GetBlockSize(req bool, res *int) error {
	if req {
		*res = nameNode.BlockSize
	}
	return nil
}

// func (nameNode *NameNodeService) GetMetadataFromWrite(req *WriteRequest, res *[]Metadata) error {
// 	nameNode.FileToBlocks[req.FileName] = []string{}
// 	numBlocks := int(int(req.FileSize) / int(nameNode.BlockSize))

// 	return nil
// }

func (nameNode *NameNodeService) assignNodes(fileName string, numBlocks int) []Metadata {
	metadata := []Metadata{}

	for i := 0; i < int(numBlocks); i++ {
		blockId := uuid.NewString()
		nameNode.FileToBlocks[fileName] = append(nameNode.FileToBlocks[fileName], blockId)
		nameNode.BlocksToDataNodes[blockId] = []int{}


	}
	return metadata
}

// func (nameNode *NameNodeService) findDataNodes(dataNodes []string) {
// 	nameNode.I
// }
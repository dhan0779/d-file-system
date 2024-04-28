package namenode

import (
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
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

type ReadRequest struct {
	FileName string
}

type Metadata struct {
	Blocks []string
	BlocksToDataNodes map[string][]int
}

func New(host string, port int, blockSize int, replicationFactor int) *Service {
	return &Service{
		BlockSize: blockSize*1000000,
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

func (nameNode *Service) GetMetadataFromWrite(req *WriteRequest, res *Metadata) error {
	nameNode.FileToBlocks[req.FileName] = []string{}
	numBlocks := int(math.Ceil(float64(req.FileSize) / float64(nameNode.BlockSize)))
	*res = nameNode.assignNodes(req.FileName, numBlocks)
	nameNode.takeSnapshot()
	return nil
}

func (nameNode *Service) assignNodes(fileName string, numBlocks int) Metadata {
	metadata := Metadata{
		BlocksToDataNodes: make(map[string][]int),
	}

	dataNodePorts := make([]int, 0, len(nameNode.DataNodeIds))
	for k := range nameNode.DataNodeIds {
		dataNodePorts = append(dataNodePorts, k)
	}

	for i := 0; i < int(numBlocks); i++ {
		blockId := uuid.NewString()
		metadata.Blocks = append(metadata.Blocks, blockId)
		nameNode.FileToBlocks[fileName] = append(nameNode.FileToBlocks[fileName], blockId)
		nameNode.BlocksToDataNodes[blockId] = []int{}

		// shuffle for random distribution of load
		for j := range dataNodePorts {
			k := rand.Intn(j+1)
			dataNodePorts[j], dataNodePorts[k] = dataNodePorts[k], dataNodePorts[j]
		}

		if nameNode.ReplicationFactor > len(dataNodePorts) {
			nameNode.ReplicationFactor = len(dataNodePorts)
		}

		for j := 0; j < nameNode.ReplicationFactor; j++ {
			metadata.BlocksToDataNodes[blockId] = append(metadata.BlocksToDataNodes[blockId], dataNodePorts[j])
			nameNode.BlocksToDataNodes[blockId] = append(nameNode.BlocksToDataNodes[blockId], dataNodePorts[j])
		}	
	}
	return metadata
}

func (nameNode *Service) GetMetadataFromRead(req *ReadRequest, res *Metadata) error {
	metadata := Metadata{
		BlocksToDataNodes: make(map[string][]int),
	}
	metadata.Blocks = nameNode.FileToBlocks[req.FileName]
	for _, blockId := range metadata.Blocks {
		metadata.BlocksToDataNodes[blockId] = nameNode.BlocksToDataNodes[blockId]
	}
	*res = metadata
	return nil
}

func (nameNode *Service) takeSnapshot() {
	snapshotId := uuid.NewString()
	snapshotPath := "snapshots/" + snapshotId + ".json"
	_, err := os.Create(snapshotPath)
	if err != nil {
		log.Println("Could not create path")
	}
	content, err := json.Marshal(nameNode)
	if err != nil {
		log.Println("Could not take snapshot")
	}

	err = os.WriteFile(snapshotPath, content, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
}

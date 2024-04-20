package namenode

import (
	"log"
	"net"
	"net/rpc"
	"strconv"

	"github.com/google/uuid"
)

type NameNodeService struct {
	BlockSize uint64
	ReplicationFactor uint64
	Port uint64
	FileToBlocks map[string][]string
	BlocksToDataNodes map[string][]uint64 // replication
	// DataNodeIdToInstance map[uint]
}

type WriteRequest struct {
	FileName string
	FileSize uint64
}

type Metadata struct {

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

func (nameNode *NameNodeService) GetBlockSize(req bool, res *uint64) error {
	if req {
		*res = nameNode.BlockSize
	}
	return nil
}

func (nameNode *NameNodeService) GetMetadataFromWrite(req *WriteRequest, res *[]Metadata) error {
	nameNode.FileToBlocks[req.FileName] = []string{}
	numBlocks := uint64(uint64(req.FileSize) / uint64(nameNode.BlockSize))

	return nil
}

func (nameNode *NameNodeService) assignNodes(fileName string, numBlocks uint64) []Metadata {
	metadata := []Metadata{}

	for i := 0; i < int(numBlocks); i++ {
		blockId := uuid.NewString()
		nameNode.FileToBlocks[fileName] = append(nameNode.FileToBlocks[fileName], blockId)
		nameNode.BlocksToDataNodes[blockId] = []uint64{}


	}
	return metadata
}

// func (nameNode *NameNodeService) findDataNodes(dataNodes []string) {
// 	nameNode.I
// }
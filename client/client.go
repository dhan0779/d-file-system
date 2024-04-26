package client

import (
	"d-file-system/datanode"
	"d-file-system/namenode"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

func WriteFile(fileDirectory string, fileName string, host string, port int) {
	filePath := fileDirectory + fileName
	fi, err := os.Stat(filePath)
	if err != nil {
		log.Println("file not found!")
	}

	fileSize := int(fi.Size())

	nameNodeInstance, err := rpc.Dial("tcp", host + ":" + strconv.Itoa(port))
	if err != nil {
		log.Printf("No connection to name node at port %d\n", port)
	}
	
	wr := namenode.WriteRequest{FileName: fileName, FileSize: fileSize}
	var metadata namenode.Metadata
	err = nameNodeInstance.Call("Service.GetMetadataFromWrite", wr, &metadata)
	if err != nil {
		log.Println("unable to get metadata from write")
	}

	var blockSize int
	err = nameNodeInstance.Call("Service.GetBlockSize", true, &blockSize)
	if err != nil {
		panic("Cannot get block size from name node")
	}
	f, err := os.Open(filePath)
	if err != nil {
		panic("Could not open file")
	}

	byteStore := make([]byte, blockSize)
	for _, blockId := range metadata.Blocks {
		_, err := f.Read(byteStore)
		if err != nil {
			log.Println("Could not read bytes from file")
		}

		for _, dataNodePort := range metadata.BlocksToDataNodes[blockId] {
			dataNodeInstance, err := rpc.Dial("tcp", host + ":" + strconv.Itoa(dataNodePort))
			if err != nil {
				log.Printf("Block to DataNode - Could not connect to data node at port %d\n", dataNodePort)
			}

			res := false
			dataNodeWriteRequest := datanode.WriteRequest{BlockId: blockId, BlockData: byteStore}
			err = dataNodeInstance.Call("Service.WriteData", dataNodeWriteRequest, &res)
			if !res || err != nil {
				log.Println("Could not write block to data node")
			}
		}
	}
}

func ReadFile(fileName string, host string, port int) {
	nameNodeInstance, err := rpc.Dial("tcp", host + ":" + strconv.Itoa(port))
	if err != nil {
		log.Printf("No connection to name node at port %d\n", port)
	}

	var blockSize int
	err = nameNodeInstance.Call("Service.GetBlockSize", true, &blockSize)
	if err != nil {
		panic("Cannot get block size from name node")
	}

	rr := namenode.ReadRequest{FileName: fileName}
	var metadata namenode.Metadata
	err = nameNodeInstance.Call("Service.GetMetadataFromRead", rr, &metadata)
	if err != nil {
		log.Println("Could not get metadata to read from name node")
	}

	fileBody := make([]byte, blockSize*len(metadata.Blocks))
	for i, blockId := range metadata.Blocks {
		blockIdRead := false
		for _, dataNodePort := range metadata.BlocksToDataNodes[blockId] {
			dataNodeInstance, err := rpc.Dial("tcp", host + ":" + strconv.Itoa(dataNodePort))
			if err != nil {
				log.Printf("Could not connect to data node at port %d\n", port)
				continue
			}
			
			res := false
			readRequest := datanode.ReadRequest{BlockId: blockId, DataBuffer: make([]byte, blockSize)}
			err = dataNodeInstance.Call("Service.WriteData", readRequest, &res)
			if err != nil {
				break
			} else {
				copy(fileBody[i*blockSize:], readRequest.DataBuffer)	
				blockIdRead = true
			}
		}

		if !blockIdRead {
			panic("Could not read entire file")
		}
	}
	log.Println(fileBody[:1000])
}
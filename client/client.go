package client

import (
	"log"
	"net/rpc"
	"os"
)

func WriteFile(fileDirectory string, fileName string, nameNodeInstance *rpc.Client) {
	filePath := fileDirectory + fileName
	fi, err := os.Stat(filePath)
	if err != nil {
		log.Println("file not found!")
	}

	fileSize := int(fi.Size())
	log.Println(fileSize)

	var blockSize int
	err = nameNodeInstance.Call("NameNodeService.GetBlockSize", true, &blockSize)
	if err != nil {
		log.Println("unable to get block size")
	}

	log.Println(blockSize)

}
package client

import (
	"log"
	"net/rpc"
	"os"
	"strconv"
	"d-file-system/namenode"
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

	log.Println(metadata)
}
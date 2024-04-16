package main

import (
	"d-file-system/datanode"
	"log"
	"os"
	"strconv"
)

func main() {
	switch os.Args[1] {
	case "datanode":
		port, e := strconv.Atoi(os.Args[2])
		if e != nil {
			log.Println("Error converting port number")
			os.Exit(1)
		}
		datanode.Initialize(port)
	default:
		log.Println("Node not specified")
		os.Exit(1)
	}
}
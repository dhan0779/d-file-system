package main

import (
	"d-file-system/datanode"
	"d-file-system/namenode"
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
	case "namenode":
		port, e := strconv.Atoi(os.Args[2])
		if e != nil {
			log.Println("Error converting port number")
			os.Exit(1)
		}
		namenode.Initialize(port)
	default:
		log.Println("Node not specified")
		os.Exit(1)
	}
}
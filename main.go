package main

import (
	"d-file-system/datanode"
	"d-file-system/namenode"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

func check_port_available(host string, port string) bool {
	timeout := time.Second
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), timeout)
	if err != nil {
		return false
	}
	if conn != nil {
		defer conn.Close()
		return true
	}
	return false
}

func main() {
	switch os.Args[2] {
	case "datanode":
		port, e := strconv.Atoi(os.Args[3])
		if e != nil {
			log.Println("Error converting port number")
			os.Exit(1)
		}
		datanode.Initialize(port)
	case "namenode":
		port, e := strconv.Atoi(os.Args[3])
		if e != nil {
			log.Println("Error converting port number")
			os.Exit(1)
		}
		namenode.Initialize(port)
	case "client":
		// client.WriteFile(os.Args[3], os.Args[4], )
	default:
		log.Println("Node not specified")
		os.Exit(1)
	}
}
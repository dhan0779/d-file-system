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

func check_port_available(host string, port int) bool {
	timeout := time.Second
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), timeout)
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
	server_port := 7000
	switch os.Args[2] {
	case "datanode":
		server_port += 1
		for !check_port_available("localhost", server_port) {
			server_port += 1
		}
		datanode.Initialize(server_port)
	case "namenode":
		namenode.Initialize(server_port)
	case "client":
		// client.WriteFile(os.Args[3], os.Args[4], )
	default:
		log.Println("Node not specified")
		os.Exit(1)
	}
}
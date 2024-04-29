package main

import (
	"d-file-system/client"
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
		conn.Close()
		return true
	}
	return false
}

func main() {
	server_port := 8000
	host := "localhost"
	switch os.Args[1] {
	case "datanode":
		// port := server_port + 1
		// for !check_port_available(host, port) {
		// 	port += 1
		// }
		port, err := strconv.Atoi(os.Args[2])
		if err != nil {
			panic(err)
		}
		datanode.Initialize(host, port, server_port)
	case "namenode":
		namenode.Initialize(host, server_port)
	case "client":
		client.WriteFile(os.Args[2], os.Args[3], host, server_port)
		// time.Sleep(time.Second*5)
		// client.ReadFile(os.Args[3], host, server_port)
	default:
		log.Println("Node not specified")
		os.Exit(1)
	}
}
package datanode

import (
	"log"
	"errors"
)

type DataNodeService struct {
	Port uint64
	NameNodePort uint64
}

func Heartbeat(req bool, res *bool) error {
	if req {
		log.Println("received from Namenode")
		*res = true
		return nil
	}

	return errors.New("heartbeat request error")
} 
package client

import (
	"log"
	"os"
)

func WriteFile(fileDirectory string, fileName string) {
	filePath := fileDirectory + fileName
	fi, err := os.Stat(filePath)
	if err != nil {
		log.Println("file not found!")
	}

	fileSize := uint64(fi.Size())
	log.Println(fileSize)
}
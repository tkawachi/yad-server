package yadserver

import (
	"io"
	"log"
	"os"
)

func serveS3 ( s3path string, req *transactionRequest) {
	// TODO fetch from s3
	log.Println("Serving", s3path)
	f, err := os.OpenFile(s3path, os.O_RDONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	n, err := io.Copy(req.w, f)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(n, "bytes written")
	req.resultChan <- 0
}

package main

import (
	"fmt"
	"http"
	"log"
	"flag"
)

var addr = flag.String("addr", ":8080", "http service address")

func RootHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintln(w, "Hello, world")
}

func main() {
	http.Handle("/", http.HandlerFunc(RootHandler))
	err := http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}

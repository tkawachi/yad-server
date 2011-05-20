package main

import (
	"yadserver"
	"flag"
)

var addr = flag.String("addr", ":8080", "http service address")

func main() {
	flag.Parse()
	yadserver.Start(*addr)
}

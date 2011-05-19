package main

import (
	"fmt"
	"http"
	"log"
	"flag"
	"strconv"
	"os"
)

var addr = flag.String("addr", ":8080", "http service address")
var txChan = make(chan *transactionRequest)

type transaction struct {
	seq uint64 // TODO serealize seq to metadata store
}

type transactionRequest struct {
	c chan uint64 // Write back channel
}

func newTransactionRequest() *transactionRequest {
	return &transactionRequest{make(chan uint64)}
}

func newTransaction() *transaction {
	return new(transaction)
}

func (self *transaction) startGen(c chan *transactionRequest) {
	for {
		self.seq++
		req := <-c
		req.c <- self.seq
	}
}

func getNewTransactionId() uint64 {
	txReq := newTransactionRequest()
	txChan <- txReq
	txSeq := <-txReq.c
	return txSeq
}

func updateHandler(w http.ResponseWriter, req *http.Request) {
	c := make(chan int)
	<-c
}

func parseQuery(w http.ResponseWriter, req *http.Request, requiredParam []string) (queryString map[string][]string, err os.Error) {
	log.Println(req.URL.RawQuery)
	queryString, err = http.ParseQuery(req.URL.RawQuery)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, err)
		return
	}
	for _, param := range requiredParam {
		if queryString[param] == nil {
			errMsg := param + " parameter is required"
			log.Println(errMsg)
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintln(w, errMsg)
			err = os.EINVAL
			return
		}
	}
	return
}

func storeHandler(w http.ResponseWriter, req *http.Request) {
	queryString, err := parseQuery(w, req, []string{"path"})
	if err != nil {
		return
	}
	log.Println(queryString)
	txSeq := getNewTransactionId()
	log.Println(txSeq)
	// TODO do actual storing
	w.Header().Set("X-yad-serial-id", strconv.Uitoa64(txSeq))
}

func fetchHandler(w http.ResponseWriter, req *http.Request) {
	queryString, err := parseQuery(w, req, []string{"path"})
	if err != nil {
		return
	}
	log.Println(queryString)
	// TODO do actual metadata fetch
	metadata := map[string]string{
		"Metadata1": "abc",
		"Metadata2": "def",
	}
	for key, value := range metadata {
		w.Header().Set("X-yad-"+key, value)
	}
	if req.Method == "HEAD" {
		return
	} else if req.Method == "GET" {
		// TODO do actual fetch
		fmt.Fprintln(w, "abc")
	}
}

func moveHandler(w http.ResponseWriter, req *http.Request) {
	queryString, err := parseQuery(w, req, []string{"from", "to"})
	if err != nil {
		return
	}
	log.Println(queryString)
	txSeq := getNewTransactionId()
	log.Println(txSeq)
	// TODO do actual move
	w.Header().Set("X-yad-serial-id", strconv.Uitoa64(txSeq))
}

func main() {
	go newTransaction().startGen(txChan)
	http.Handle("/", http.HandlerFunc(updateHandler))
	http.Handle("/store", http.HandlerFunc(storeHandler))
	http.Handle("/fetch", http.HandlerFunc(fetchHandler))
	http.Handle("/move", http.HandlerFunc(moveHandler))
	err := http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}

package yadserver

import (
	"bytes"
	"fmt"
	"gob"
	"http"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	redis "github.com/simonjefford/Go-Redis/src"
)

const (
	txCounterKey = "tx.counter"
	txListKey    = "tx.list"
	txKeyFormat  = "tx:%d"
	pathKey      = "path:%s"
	hashKey      = "hash:%s"

	headerPrefix    = "X-Yad-"
	txHeader        = headerPrefix + "Last-Serial-Id"
	dataFoundHeader = headerPrefix + "Data-Found"
	hashHeader      = headerPrefix + "Meta-Sha256"
)

const (
	redisReqStore = iota
	redisReqMove
	redisReqFetch
)

var txChan = make(chan *transactionRequest)

type transaction struct {
	redisClient redis.Client
}

type transactionRequest struct {
	kind        int      // redisReqXXX
	resultChan  chan int // Write back result
	w           http.ResponseWriter
	req         *http.Request
	queryString map[string][]string
}

func NormalizePath(path string) string {
	for len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	for {
		new_path := strings.Replace(path, "//", "/", -1)
		log.Println(new_path, path)
		if new_path == path {
			break
		}
		path = new_path
	}
	return path
}

func newTransactionRequest(kind int, w http.ResponseWriter, req *http.Request, queryString map[string][]string) *transactionRequest {
	return &transactionRequest{kind, make(chan int), w, req, queryString}
}

func newTransaction() *transaction {
	return new(transaction)
}

func (self *transaction) incrTxCounter() (int64, os.Error) {
	txSeq, err := self.redisClient.Incr(txCounterKey)
	if err != nil {
		log.Fatal("Incr txCounterKey:", err)
	}
	return txSeq, err
}

func makeS3Path(req *transactionRequest) string {
	// TODO create real s3 path
	return "/tmp/s3/" + req.req.Header[hashHeader][0]
}

func storeBinary(req *transactionRequest, s3path string) {
	// TODO store to s3
	f, err := os.OpenFile(s3path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}
	n, err := io.Copy(f, req.req.Body)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(n, " bytes written")
	req.resultChan <- 0
}

func (self *transaction) store(req *transactionRequest) {
	txSeq, _ := self.incrTxCounter()
	log.Println("txSeq", txSeq)
	req.w.Header().Set(txHeader, strconv.Itoa64(txSeq))

	metadata := make(map[string][]string)
	if req.req.Method == "GET" || req.req.Method == "HEAD" {
		storedVal, err := self.redisClient.Get(
			fmt.Sprintf(pathKey, req.req.Header[hashHeader]))
		if err != nil {
			log.Fatal("Get() for store:", err)
		}
		log.Println(storedVal)
		if len(storedVal) == 0 {
			req.w.Header().Set(dataFoundHeader, "no")
		} else {
			req.w.Header().Set(dataFoundHeader, "yes")
		}
		for k, v := range req.req.Header {
			if strings.HasPrefix(k, headerPrefix) {
				log.Println(k, v)
				metadata[k] = v
			}
		}
	} else if req.req.Method == "POST" {
		for k, v := range req.req.Header {
			if strings.HasPrefix(k, headerPrefix) ||
				k == "Content-Type" {
				log.Println(k, v)
				metadata[k] = v
			}
		}
		metadata["Content-Length"] = []string{
			strconv.Itoa64(req.req.ContentLength)}
		s3path := makeS3Path(req)
		self.redisClient.Set(fmt.Sprintf(hashKey,
			req.req.Header[hashHeader][0]),
			[]byte(s3path))
		go storeBinary(req, s3path)
	} else {
		log.Println("Unsupported method:", req.req.Method)
		req.w.WriteHeader(http.StatusBadRequest)
		req.resultChan <- -1
		return
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(req.req.Header)
	self.redisClient.Set(fmt.Sprintf(pathKey,
		req.queryString["path"]),
		buf.Bytes())

	self.saveTransaction(txSeq,
		fmt.Sprintf("STORED\t%s", req.queryString["path"]))
	if req.req.Method == "GET" || req.req.Method == "HEAD" {
		// for POST, resultChan is written from storeBinary()
		req.resultChan <- 0
	}
	/*
		b := network.Bytes()
		network2 := bytes.NewBuffer(b)
		dec := gob.NewDecoder(network2)
		var m map[string][]string
		dec.Decode(&m)
		log.Println(m)
	*/
}

func (self *transaction) saveTransaction(txSeq int64, tx string) {
	log.Println("saveTransaction()")
	log.Println(fmt.Sprintf(txKeyFormat, txSeq), []uint8(tx))
	err := self.redisClient.Set(fmt.Sprintf(txKeyFormat, txSeq), []uint8(tx))
	if err != nil {
		log.Fatal("redis Set():", err)
	}
	err = self.redisClient.Rpush(txListKey, []uint8(strconv.Itoa64(txSeq)))
	if err != nil {
		log.Fatal("redis Set():", err)
	}
	//fmt.Println(self.redisClient.Lrange(txListKey, 0, -1))
}

func (self *transaction) start(c chan *transactionRequest) {
	self.redisClient = connectToRedis()
	for {
		req := <-c
		log.Println(req, redisReqStore)
		if req.kind == redisReqStore {
			self.store(req)
		} else if req.kind == redisReqMove {
		} else if req.kind == redisReqFetch {
		}
	}
}

func helloHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintln(w, "Hello, world!")
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
	if len(req.Header[hashHeader]) != 1 {
		errMsg := "One " + hashHeader + " header should exist"
		log.Println(errMsg)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, errMsg)
		return
	}
	txReq := newTransactionRequest(redisReqStore, w, req, queryString)
	txChan <- txReq
	<-txReq.resultChan
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
	// TODO do actual move
}

func connectToRedis() redis.Client {
	spec := redis.DefaultSpec()
	redisClient, e := redis.NewSynchClientWithSpec(spec)
	if e != nil {
		log.Fatal("failed to create the client", e)
	}
	return redisClient
}

func Start(addr string) {
	go newTransaction().start(txChan)
	http.Handle("/", http.HandlerFunc(helloHandler))
	http.Handle("/update", http.HandlerFunc(updateHandler))
	http.Handle("/store", http.HandlerFunc(storeHandler))
	http.Handle("/fetch", http.HandlerFunc(fetchHandler))
	http.Handle("/move", http.HandlerFunc(moveHandler))
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}

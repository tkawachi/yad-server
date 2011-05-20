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
	redisReqUpdate
)

var txChan = make(chan *transactionRequest)
var updateCliChan = make(chan *updateClient)
var updateNotiChan = make(chan *updateNotification)

type transaction struct {
	redisClient redis.Client
}

type transactionRequest struct {
	kind        int      // redisReqXXX
	resultChan  chan int // Write back result
	w           http.ResponseWriter
	req         *http.Request
	queryString map[string][]string
	path        string
}

func (self *transactionRequest)hasTxHeader() bool {
	return len(self.req.Header[txHeader]) > 0
}

func (self *transactionRequest)getTxHeader() int64 {
	if !self.hasTxHeader() {
		return -1
	}
	i, err := strconv.Atoi64(self.req.Header[txHeader][0])
	if err != nil {
		log.Println(err)
		return -1
	}
	return i
}

type metadata struct {
	Headers       map[string][]string
	ContentLength int64
	Hash          string
}

func newMetadata() *metadata {
	return &metadata{make(map[string][]string), 0, ""}
}

// Normalize path to store on the metadata DB
func normalizePath(path string) string {
	for len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	for {
		new_path := strings.Replace(path, "//", "/", -1)
		if new_path == path {
			break
		}
		path = new_path
	}
	return path
}

func newTransactionRequest(kind int, w http.ResponseWriter, req *http.Request, queryString map[string][]string) *transactionRequest {
	txReq := transactionRequest{
		kind, make(chan int), w, req, queryString, ""}
	if len(txReq.queryString["path"]) > 0 {
		txReq.path = normalizePath(queryString["path"][0])
	}
	return &txReq
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

func (self *transaction) curTxCounter() (int64, os.Error) {
	txSeq, err := self.redisClient.Get(txCounterKey)
	if err != nil {
		log.Fatal("Current txCounterKey:", err)
	}
	return strconv.Atoi64(string(txSeq))
}

func makeS3Path(req *transactionRequest) string {
	// TODO create real s3 path
	return "/tmp/s3/" + req.req.Header[hashHeader][0]
}

func storeBinary(req *transactionRequest, s3path string) {
	// TODO store to s3
	f, err := os.OpenFile(s3path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatal(err)
	}
	n, err := io.Copy(f, req.req.Body)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(n, " bytes written")
}

func (self *transaction) store(req *transactionRequest) {
	txSeq, _ := self.incrTxCounter()
	log.Println("txSeq", txSeq)
	req.w.Header().Set(txHeader, strconv.Itoa64(txSeq))

	md := newMetadata()
	md.Hash = req.req.Header[hashHeader][0]
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
				md.Headers[k] = v
			}
		}
	} else if req.req.Method == "POST" {
		for k, v := range req.req.Header {
			if strings.HasPrefix(k, headerPrefix) ||
				k == "Content-Type" {
				log.Println(k, v)
				md.Headers[k] = v
			}
		}
		md.ContentLength = req.req.ContentLength
		s3path := makeS3Path(req)
		self.redisClient.Set(fmt.Sprintf(hashKey,
			req.req.Header[hashHeader][0]),
			[]byte(s3path))
	} else {
		log.Println("Unsupported method:", req.req.Method)
		req.w.WriteHeader(http.StatusBadRequest)
		req.resultChan <- -1
		return
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	e := enc.Encode(md)
	if e != nil {
		log.Fatal(e)
	}
	log.Println("encoded metadata", md)
	self.redisClient.Set(fmt.Sprintf(pathKey, req.path),
		buf.Bytes())

	txContent := fmt.Sprintf("STORED\t%s", req.path)
	self.saveTransaction(txSeq, txContent)
	updateNotiChan <- &updateNotification{txSeq, txContent}
	req.resultChan <- 0
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

func (self *transaction) fetch(req *transactionRequest) {
	txSeq, _ := self.curTxCounter()
	log.Println("txSeq", txSeq)
	req.w.Header().Set(txHeader, strconv.Itoa64(txSeq))

	b, err := self.redisClient.Get(fmt.Sprintf(pathKey, req.path))
	if err != nil {
		log.Fatal(err)
	}
	log.Println("bytes to decode", b)
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	var md metadata
	e := dec.Decode(&md)
	if e != nil {
		log.Fatal(e)
	}
	log.Println("decoded metadata", md)
	for k, v := range md.Headers {
		for _, v2 := range v {
			req.w.Header().Set(k, v2)
		}
	}

	if req.req.Method == "GET" {
		b, err = self.redisClient.Get(fmt.Sprintf(hashKey, md.Hash))
		if err != nil {
			log.Fatal(err)
		}
		s3path := string(b)
		log.Println(s3path)
		go serveS3(s3path, req)
	} else {
		req.resultChan <- 0
	}
}

func (self *transaction) update(req *transactionRequest) {
	l, err := self.redisClient.Lrange(txListKey, 0, -1)
	if err != nil {
		log.Fatal(err)
	}
	isWroteContents := false
	for _, x := range l {
		k := string(x)
		seq, e := strconv.Atoi64(k)
		if e != nil {
			log.Println(k, "can't be converted to int64")
			continue
		}
		if req.getTxHeader() < seq {
			contents, err := self.redisClient.Get(
				fmt.Sprintf(txKeyFormat, seq))
			if err != nil {
				log.Fatal(err)
			}
			fmt.Fprintf(req.w, "%d\t%s\n", seq, string(contents))
			log.Println("Send client:", string(contents))
			isWroteContents = true
		}
	}
	if isWroteContents {
		req.resultChan <- 0
	} else {
		cli := updateClient{req.w, req.req, req.resultChan}
		updateCliChan <- &cli
	}
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
			self.fetch(req)
		} else if req.kind == redisReqUpdate {
			self.update(req)
		}
	}
}

func helloHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintln(w, "Hello, world!")
}

func updateHandler(w http.ResponseWriter, req *http.Request) {
	queryString, err := parseQuery(w, req, []string{})
	if err != nil {
		return
	}
	log.Println(queryString)
	txReq := newTransactionRequest(redisReqUpdate, w, req, queryString)
	txChan <- txReq
	<-txReq.resultChan
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
		if queryString[param] == nil || len(queryString[param]) == 0 {
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
	s3path := makeS3Path(txReq)
	storeBinary(txReq, s3path)
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
	txReq := newTransactionRequest(redisReqFetch, w, req, queryString)
	txChan <- txReq
	<-txReq.resultChan
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
	go updateRoutine(updateCliChan, updateNotiChan)
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

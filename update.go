package yadserver

import (
	"http"
	"fmt"
)

type updateClient struct {
	w   http.ResponseWriter
	req *http.Request
	ch  chan int
}

type updateNotification struct {
	txSeq   int64
	content string
}

func updateRoutine(chClient chan *updateClient, chNotification chan *updateNotification) {
	clientList := make([]*updateClient, 0, 100)
	for {
		select {
		case cli := <-chClient:
			clientList = append(clientList, cli)
		case noti := <-chNotification:
			for _, cli := range clientList {
				fmt.Fprintf(cli.w, "%d\t%s\n",
					     noti.txSeq, noti.content)
				cli.ch <- 0
			}
			// XXX thread unsafe ?
			clientList = make([]*updateClient, 0, 100)
		}
	}
}

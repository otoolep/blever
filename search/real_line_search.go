package main

import (
	"log"
	"net"
	"os"
	"time"

	"github.com/blevesearch/bleve"
)

const indexPath = "index.bleve"
const log1 = "<134>1 2013-09-04T10:25:52.618085 ubuntu sshd 1999 - password accepted for user root"

type Log struct {
	ID            string
	ReceptionTime time.Time
	ReferenceTime time.Time
	SourceIP      net.IP

	Priority int
	Facility int
	Hostname string
	AppName  string
	PID      int
	Message  string
}

func main() {
	if _, err := os.Stat(indexPath); err == nil {
		log.Printf("removing existing index at %s", indexPath)
		os.RemoveAll(indexPath)
	}

	mapping := bleve.NewIndexMapping()
	index, err := bleve.New(indexPath, mapping)
	if err != nil {
		log.Fatalf("failed to create index: %s", err.Error())
	}

	msg := Log{
		ID:            "1",
		ReceptionTime: time.Now(),
		ReferenceTime: time.Now(),
		SourceIP:      net.ParseIP("127.0.0.1"),
		Priority:      134,
		Facility:      1,
		Hostname:      "localhost",
		AppName:       "sshd",
		PID:           1999,
		Message:       "password accepted for user root",
	}
	if err = index.Index(msg.ID, msg); err != nil {
		log.Fatalf("failed to index: %s", err.Error())
	}
	log.Println("finished indexing")

}

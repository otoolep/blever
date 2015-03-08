package main

import (
	"log"
	"os"

	"github.com/blevesearch/bleve"
)

const indexPath = "index.bleve"
const log1 = "<134>1 2013-09-04T10:25:52.618085 ubuntu sshd 1999 - password accepted for user root"
const log2 = "<134>1 2013-09-04T10:25:52.618085 ubuntu sshd 1999 - password rejected for user philip"
const log3 = "this is a plain message"

type LogLine struct {
	ID   string
	Line string
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

	var data LogLine

	// log 3
	data = LogLine{ID: "3", Line: log3}
	if err = index.Index(data.ID, data); err != nil {
		log.Fatalf("failed to index: %s", err.Error())
	}
	log.Println("finished indexing")
	q := "message"
	log.Printf(`searching for "%s"`, q)
	query := bleve.NewMatchQuery(q)
	search := bleve.NewSearchRequest(query)
	searchResults, err := index.Search(search)
	if err != nil {
		log.Println("error:", err.Error())
		return
	}
	log.Println(searchResults)

	// log 1
	data = LogLine{ID: "1", Line: log1}
	if err = index.Index(data.ID, data); err != nil {
		log.Fatalf("failed to index: %s", err.Error())
	}
	log.Println("finished indexing")
	q = "password accepted for user root"
	log.Printf(`searching for "%s"`, q)
	query = bleve.NewMatchQuery(q)
	search = bleve.NewSearchRequest(query)
	searchResults, err = index.Search(search)
	if err != nil {
		log.Println("error:", err.Error())
		return
	}
	log.Println(searchResults)

}

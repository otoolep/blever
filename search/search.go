package main

import (
	"log"
	"os"

	"github.com/blevesearch/bleve"
)

const indexPath = "index.bleve"
const log1 = "<134>1 2013-09-04T10:25:52.618085 ubuntu sshd 1999 - password accepted for user root"
const log2 = "password rejected for user philip"
const log3 = "this is a plain message"

type LogLine struct {
	ID   string
	Line string
}

type LogLineWithAppName struct {
	ID      string
	Line    string
	AppName string
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

	// log 2
	data2 := LogLineWithAppName{ID: "2", Line: log2, AppName: "pamd"}
	if err = index.Index(data2.ID, data2); err != nil {
		log.Fatalf("failed to index: %s", err.Error())
	}
	log.Println("finished indexing")
	q2 := "pamd"
	log.Printf(`searching for "%s"`, q2)
	query2 := bleve.NewPhraseQuery([]string{q2}, "AppName")
	search = bleve.NewSearchRequest(query2)
	searchResults, err = index.Search(search)
	if err != nil {
		log.Println("error:", err.Error())
		return
	}
	if len(searchResults.Hits) > 0 {
		for _, h := range searchResults.Hits {
			log.Println(">>>>", h.ID)
		}
	} else {
		log.Println("no hits")
	}

	// log 2 -- again
	query3 := bleve.NewPhraseQuery([]string{q2}, "Line")
	search = bleve.NewSearchRequest(query3)
	searchResults, err = index.Search(search)
	if err != nil {
		log.Println("error:", err.Error())
		return
	}
	if len(searchResults.Hits) > 0 {
		for _, h := range searchResults.Hits {
			log.Println(">>>>", h.ID)
		}
	} else {
		log.Println("no hits")
	}

	// log 2 -- again
	query4 := bleve.NewPhraseQuery([]string{q2}, "appname")
	search = bleve.NewSearchRequest(query4)
	searchResults, err = index.Search(search)
	if err != nil {
		log.Println("error:", err.Error())
		return
	}
	if len(searchResults.Hits) > 0 {
		for _, h := range searchResults.Hits {
			log.Println(">>>>", h.ID)
		}
	} else {
		log.Println("no hits")
	}
}

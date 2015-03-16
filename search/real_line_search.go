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
	Host     string
	AppName  string
	PID      int
	Message  string
}

func main() {
	if _, err := os.Stat(indexPath); err == nil {
		log.Printf("removing existing index at %s", indexPath)
		os.RemoveAll(indexPath)
	}

	index, err := bleve.New(indexPath, bleve.NewIndexMapping())
	//index, err := bleve.New(indexPath, buildLogLineMapping())
	if err != nil {
		log.Fatalf("failed to create index: %s", err.Error())
	}

	now := time.Now().UTC()
	msg := Log{
		ID:            "1",
		ReceptionTime: now,
		ReferenceTime: now.Add(-1 * time.Hour),
		SourceIP:      net.ParseIP("127.0.0.1"),
		Priority:      134,
		Facility:      1,
		Host:          "localhost",
		AppName:       "sshd",
		PID:           1999,
		Message:       "password accepted for user root",
	}
	log.Println("Message:", msg)
	if err = index.Index(msg.ID, msg); err != nil {
		log.Fatalf("failed to index: %s", err.Error())
	}
	log.Println("finished indexing")

	q := "sshd"
	log.Printf(`MatchQuery searching for "%s"`, q)
	query := bleve.NewMatchQuery(q)
	search := bleve.NewSearchRequest(query)
	searchResults, err := index.Search(search)
	if err != nil {
		log.Println("error:", err.Error())
		return
	}
	log.Println(searchResults)

	q = "127.0.0.1"
	log.Printf(`MatchQuery searching for "%s"`, q)
	query = bleve.NewMatchQuery(q)
	search = bleve.NewSearchRequest(query)
	searchResults, err = index.Search(search)
	if err != nil {
		log.Println("error:", err.Error())
		return
	}
	log.Println(searchResults)

	q = "1999"
	log.Printf(`MatchQuery searching for "%s"`, q)
	query = bleve.NewMatchQuery(q)
	search = bleve.NewSearchRequest(query)
	searchResults, err = index.Search(search)
	if err != nil {
		log.Println("error:", err.Error())
		return
	}
	log.Println(searchResults)

	q = "localhost"
	log.Printf(`MatchQuery searching for "%s"`, q)
	query = bleve.NewMatchQuery(q)
	search = bleve.NewSearchRequest(query)
	searchResults, err = index.Search(search)
	if err != nil {
		log.Println("error:", err.Error())
		return
	}
	log.Println(searchResults)

	q = "localhost"
	log.Printf(`NewPhraseQuery searching for "%s in field Host"`, q)
	fieldQuery := bleve.NewPhraseQuery([]string{q}, "Host")
	search = bleve.NewSearchRequest(fieldQuery)
	searchResults, err = index.Search(search)
	if err != nil {
		log.Println("error:", err.Error())
		return
	}
	log.Println(searchResults)

	q = "LOCALhost"
	log.Printf(`NewPhraseQuery searching for "%s in field Host"`, q)
	fieldQuery = bleve.NewPhraseQuery([]string{q}, "Host")
	search = bleve.NewSearchRequest(fieldQuery)
	searchResults, err = index.Search(search)
	if err != nil {
		log.Println("error:", err.Error())
		return
	}
	log.Println(searchResults)

	q = "local"
	log.Printf(`NewPhraseQuery searching for "%s in field Host"`, q)
	fieldQuery = bleve.NewPhraseQuery([]string{q}, "Host")
	search = bleve.NewSearchRequest(fieldQuery)
	searchResults, err = index.Search(search)
	if err != nil {
		log.Println("error:", err.Error())
		return
	}
	log.Println(searchResults)

	q = "localhost"
	log.Printf(`NewPhraseQuery searching for "%s in field AppName"`, q)
	fieldQuery = bleve.NewPhraseQuery([]string{q}, "AppName")
	search = bleve.NewSearchRequest(fieldQuery)
	searchResults, err = index.Search(search)
	if err != nil {
		log.Println("error:", err.Error())
		return
	}
	log.Println(searchResults)

	startTime := now.Add(-2 * time.Hour).Format(time.RFC3339)
	endTime := now.Add(-60 * time.Second).Format(time.RFC3339)
	log.Printf("querying between %s and %s", startTime, endTime)
	qd := bleve.NewDateRangeQuery(&startTime, &endTime)
	search = bleve.NewSearchRequest(qd)
	search.Fields = []string{"ReferenceTime"}
	searchResults, err = index.Search(search)
	if err != nil {
		log.Println("error:", err.Error())
		return
	}
	log.Println(searchResults)
}

func buildLogLineMapping() *bleve.IndexMapping {
	// a generic reusable mapping for english text
	standardJustIndexed := bleve.NewTextFieldMapping()
	standardJustIndexed.Store = false
	standardJustIndexed.IncludeInAll = false
	standardJustIndexed.IncludeTermVectors = false
	standardJustIndexed.Analyzer = "standard"

	timeJustIndexed := bleve.NewDateTimeFieldMapping()
	timeJustIndexed.Store = false
	timeJustIndexed.IncludeInAll = false
	timeJustIndexed.IncludeTermVectors = false

	articleMapping := bleve.NewDocumentMapping()

	articleMapping.AddFieldMappingsAt("Message", standardJustIndexed)
	articleMapping.AddFieldMappingsAt("ReferenceTime", timeJustIndexed)
	articleMapping.AddFieldMappingsAt("ReceptionTime", timeJustIndexed)

	indexMapping := bleve.NewIndexMapping()
	indexMapping.DefaultMapping = articleMapping
	indexMapping.DefaultAnalyzer = "standard"
	return indexMapping
}

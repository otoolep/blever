package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
)

var batchSize = flag.Int("batchSize", 100, "batch size for indexing")
var dupe = flag.Int("dupe", 1, "line dupe factor")
var shards = flag.Int("shards", 1, "number of index shards")
var maxprocs = flag.Int("maxprocs", 1, "GOMAXPROCS")
var indexPath = flag.String("index", "logs.bleve", "index path")
var logsPath = flag.String("logs", "zoto_sample_logs.log.100k", "path to log file")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write mem profile to file")

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*maxprocs)

	// To profile, execute 'go tool pprof simple_log_index <pprof file>'
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}

	// Attempt to open the file.
	log.Printf("Opening log file %s", *logsPath)
	logs, err := os.Open(*logsPath)
	if err != nil {
		log.Fatalf("failed to open logs: %s", err.Error())
	}

	// Read the lines into memory.
	lines := make([]string, 0, 100000)
	scanner := bufio.NewScanner(logs)
	totalLen := 0
	for scanner.Scan() {
		l := scanner.Text()
		totalLen = totalLen + len(l)
		for n := 0; n < *dupe; n++ {
			lines = append(lines, l)
		}
	}

	lines = lines[:(len(lines) / (*shards))]

	// Index them!
	log.Print("indexing commencing....")

	startTime := time.Now()

	// Create indexers.
	var wg sync.WaitGroup
	for n := 0; n < *shards; n++ {
		wg.Add(1)
		err = createIndexer(n*len(lines), *indexPath+strconv.Itoa(n), *batchSize, &wg, lines)
		if err != nil {
			log.Fatalf("failed to create indexing channel %d: %s", n, err.Error())
		}
		log.Printf("created indexing channel %d", n)
	}

	totalIndexed := len(lines) * *shards

	log.Print("waiting for indexing channels to finish.")
	wg.Wait()

	pprof.StopCPUProfile()

	indexDuration := time.Since(startTime)
	timePerDoc := float64(indexDuration) / float64(totalIndexed)

	log.Print("indexing complete.")
	log.Printf("GOMAXPROCS was: %d", runtime.GOMAXPROCS(-1))
	log.Printf("average line length: %d", totalLen/len(lines))
	log.Printf("indexed %d documents, in %.2fs (average %.2fms/doc). %d/sec", totalIndexed, indexDuration.Seconds(),
		timePerDoc/float64(time.Millisecond), int(float64(totalIndexed)/indexDuration.Seconds()))

	// Create an index alias, add all indexes, and then match all docs. This is to verify that all
	// indexing actually took place.
	var indexes = make([]bleve.Index, 0)
	alias := bleve.NewIndexAlias()
	for n := 0; n < *shards; n++ {
		index, err := bleve.Open(*indexPath + strconv.Itoa(n))
		if err != nil {
			log.Fatalf("failed to create index for alias %d: %s", n, err.Error())

		}
		indexes = append(indexes, index)
	}
	alias.Add(indexes...)

	query := bleve.NewMatchAllQuery()
	search := bleve.NewSearchRequest(query)
	searchResults, err := alias.Search(search)
	if err != nil {
		log.Println("error:", err.Error())
		return
	}
	log.Println(searchResults)

	log.Println("attempting to fetch document by ID")
	doc, err := indexes[0].GetInternal([]byte("467"))
	if err != nil {
		log.Fatalf("failed to get doc by ID: %s", err.Error())
	}
	log.Println(string(doc))

}

func createIndexer(offset int, indexPath string, batchSize int, wg *sync.WaitGroup, lines []string) error {
	if _, err := os.Stat(indexPath); err == nil {
		log.Printf("removing existing index at %s", indexPath)
		os.RemoveAll(indexPath)
	}
	log.Printf("index created at %s with doc ID offset of %d", indexPath, offset)

	//mapping := bleve.NewIndexMapping()
	index, err := bleve.New(indexPath, buildLogLineMapping())
	if err != nil {
		return err
	}

	go func() {
		batch := bleve.NewBatch()
		batchCount := 0
		numIndex := 0

		for _, l := range lines {
			data := struct {
				Line string
			}{
				Line: l,
			}

			batch.Index(strconv.Itoa(numIndex), data)
			batch.SetInternal([]byte(strconv.Itoa(numIndex+offset)), []byte(l))
			batchCount++
			numIndex++

			if batchCount >= batchSize {
				if err := index.Batch(batch); err != nil {
					log.Fatalf("failed to index batch of lines: %s", err.Error())
				}
				log.Printf("batch written to %s", indexPath)
				batch = bleve.NewBatch()
				batchCount = 0
			}
		}

		// Remaining sub-batch count.
		if batchCount > 0 {
			if err := index.Batch(batch); err != nil {
				log.Fatalf("failed to index batch of lines: %s", err.Error())
			}
		}

		log.Printf("indexing channel for shard %s done, %d lines indexed", indexPath, numIndex)
		index.Close()
		wg.Done()
	}()

	return nil
}

func buildLogLineMapping() *bleve.IndexMapping {
	// a generic reusable mapping for english text
	standardJustIndexed := bleve.NewTextFieldMapping()
	standardJustIndexed.Store = false
	standardJustIndexed.IncludeInAll = false
	standardJustIndexed.IncludeTermVectors = false
	standardJustIndexed.Analyzer = "standard"

	articleMapping := bleve.NewDocumentMapping()

	// line
	articleMapping.AddFieldMappingsAt("Line", standardJustIndexed)

	indexMapping := bleve.NewIndexMapping()
	indexMapping.DefaultMapping = articleMapping
	indexMapping.DefaultAnalyzer = "standard"
	return indexMapping
}

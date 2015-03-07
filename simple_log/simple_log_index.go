package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/blevesearch/bleve"
)

var batchSize = flag.Int("batchSize", 100, "batch size for indexing")
var indexPath = flag.String("index", "logs.bleve", "index path")
var logsPath = flag.String("logs", "zoto_sample_logs.log.100k", "path to log file")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write mem profile to file")

func main() {
	flag.Parse()

	// Attempt to open the file.
	log.Printf("Opening log file %s", *logsPath)
	logs, err := os.Open(*logsPath)
	if err != nil {
		log.Fatalf("failed to open logs: %s", err.Error())
	}

	// Read the lines into memory.
	lines := make([]string, 0, 100000)
	scanner := bufio.NewScanner(logs)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	// Index them!
	if _, err := os.Stat(*indexPath); err == nil {
		log.Printf("removing existing index at %s", *indexPath)
		os.RemoveAll(*indexPath)
	}
	log.Print("indexing commencing....")
	mapping := bleve.NewIndexMapping()
	index, err := bleve.New(*indexPath, mapping)

	startTime := time.Now()

	batch := bleve.NewBatch()
	batchCount := 0
	totalIndexed := 0
	for i, l := range lines[:10000] {
		data := struct {
			Line string
		}{
			Line: l,
		}
		batch.Index(strconv.Itoa(i), data)
		batchCount++
		totalIndexed++

		if batchCount >= *batchSize {
			if err := index.Batch(batch); err != nil {
				log.Fatalf("failed to index batch of lines: %s", err.Error())
			}
			log.Printf("indexed batch %d", i)
			batch = bleve.NewBatch()
			batchCount = 0
		}
	}

	indexDuration := time.Since(startTime)
	indexDurationSeconds := float64(indexDuration) / float64(time.Second)
	timePerDoc := float64(indexDuration) / float64(totalIndexed)

	log.Print("indexing complete.")
	log.Printf("Indexed %d documents, in %.2fs (average %.2fms/doc). %f/sec", totalIndexed, indexDurationSeconds, timePerDoc/float64(time.Millisecond), float64(totalIndexed)/indexDurationSeconds)
}

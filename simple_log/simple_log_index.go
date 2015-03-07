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

	// Index them!
	log.Print("indexing commencing....")

	// Create indexers.
	var wg sync.WaitGroup
	logChans := make([]chan interface{}, *shards)
	for n := 0; n < *shards; n++ {
		wg.Add(1)
		logChans[n], err = createIndexer(*indexPath+strconv.Itoa(n), *batchSize, &wg)
		if err != nil {
			log.Fatalf("failed to create indexing channel %d: %s", n, err.Error())
		}
		log.Printf("created indexing channel %d", n)
	}

	startTime := time.Now()

	totalIndexed := 0
	for i, l := range lines[:20000] {
		logChans[i%*shards] <- l
		totalIndexed++
	}

	for _, c := range logChans {
		close(c)
	}

	log.Print("waiting for indexing channels to finish.")
	wg.Wait()

	pprof.StopCPUProfile()

	indexDuration := time.Since(startTime)
	indexDurationSeconds := float64(indexDuration) / float64(time.Second)
	timePerDoc := float64(indexDuration) / float64(totalIndexed)

	log.Print("indexing complete.")
	log.Printf("average line length: %d", totalLen/len(lines))
	log.Printf("indexed %d documents, in %.2fs (average %.2fms/doc). %f/sec", totalIndexed, indexDurationSeconds, timePerDoc/float64(time.Millisecond), float64(totalIndexed)/indexDurationSeconds)
}

func createIndexer(indexPath string, batchSize int, wg *sync.WaitGroup) (chan interface{}, error) {
	logChan := make(chan interface{})

	if _, err := os.Stat(indexPath); err == nil {
		log.Printf("removing existing index at %s", indexPath)
		os.RemoveAll(indexPath)
	}

	mapping := bleve.NewIndexMapping()
	index, err := bleve.New(indexPath, mapping)
	if err != nil {
		return logChan, err
	}

	go func() {
		batch := bleve.NewBatch()
		batchCount := 0
		numIndex := 0

		for l := range logChan {
			data := struct {
				Line string
			}{
				Line: l.(string),
			}

			batch.Index(strconv.Itoa(numIndex), data)
			batchCount++
			numIndex++

			if batchCount >= batchSize {
				if err := index.Batch(batch); err != nil {
					log.Fatalf("failed to index batch of lines: %s", err.Error())
				}
				batch = bleve.NewBatch()
				batchCount = 0
			}
		}
		log.Printf("indexing channel for shard %s done, %d lines indexed", indexPath, numIndex)
		wg.Done()
	}()

	return logChan, nil
}

package main

import (
	"flag"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ChronixDB/chronix.go/chronix"
	"github.com/ChronixDB/chronix.ingester/ingester"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

func main() {
	var (
		listenAddr            = flag.String("listen-addr", ":8080", "The address to listen on.")
		storageUrl            = flag.String("url", "http://localhost:8983/solr/chronix", "The URL of the Chronix endpoint.")
		kind                  = flag.String("kind", "solr", "Possible values are: 'solr' or 'elastic'")
		esWithIndex           = flag.Bool("es.withIndex", true, "Creates an index (only used with elastic search")
		esDeleteIndexIfExists = flag.Bool("es.deleteIndexIfExists", false, "Deletes the index only with es.withIndex=true")
		esSniffNodes          = flag.Bool("es.sniffNodes", true, "Should elastic client sniff for ES nodes (only used with elastic search)")
		commitWithin          = flag.Duration("chronix-commit-within", 5*time.Second, "The duration after which updates to Chronix should be committed.")
		maxChunkAge           = flag.Duration("max-chunk-age", time.Hour, "The maximum age of a chunk before it is closed and persisted.")
		checkpointFile        = flag.String("checkpoint-file", "checkpoint.db", "The path to the checkpoint file.")
		checkpointInterval    = flag.Duration("checkpoint-interval", 5*time.Minute, "The interval between checkpoints.")
		flushOnShutdown       = flag.Bool("flush-on-shutdown", false, "Whether to flush all chunks to Chronix on shutdown, rather than saving them to a checkpoint. A checkpoint will still be written, but will be empty.")
		createStatistics      = flag.Bool("create-statistics", false, "Whether to store some additional statistical data for each data chunk.")
	)
	flag.Parse()

	var storageClient chronix.StorageClient
	if *kind == "solr" {

		u, err := url.Parse(*storageUrl)
		if err != nil {
			log.Fatalln("Failed to parse Chronix URL:", err)
		}
		storageClient = chronix.NewSolrStorage(u, nil)
	} else if *kind == "elastic" {

		storageClient = chronix.NewElasticStorage(storageUrl, esWithIndex, esDeleteIndexIfExists, esSniffNodes)
	} else {
		log.Fatalln("Kind parameter unknown:", *kind)
	}

	var client chronix.Client
	if *createStatistics {
		client = chronix.NewWithStatistics(storageClient)
	} else {
		client = chronix.New(storageClient)
	}

	ing, err := ingester.NewIngester(
		ingester.Config{
			MaxChunkAge:        *maxChunkAge,
			CheckpointFile:     *checkpointFile,
			CheckpointInterval: *checkpointInterval,
			FlushOnShutdown:    *flushOnShutdown,
		},
		&chronixStore{
			chronix:      client,
			commitWithin: *commitWithin,
		},
	)
	if err != nil {
		log.Fatalln("Failed to create ingester:", err)
	}
	defer ing.Stop()
	prometheus.Register(ing)

	http.Handle("/ingest", ingestHandler(ing))
	http.Handle("/metrics", prometheus.Handler())

	log.Infoln("Listening on", *listenAddr)
	go func() {
		log.Fatalln(http.ListenAndServe(*listenAddr, nil))
	}()

	term := make(chan os.Signal)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	<-term
	log.Infoln("Received SIGTERM, exiting gracefully...")
}

package main

import (
	"flag"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/ChronixDB/chronix.go/chronix"
	"github.com/ChronixDB/chronix.ingester/ingester"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	var (
		listenAddr  = flag.String("listen-addr", ":8080", "The address to listen on.")
		chronixURL  = flag.String("chronix-url", "http://localhost:8983/solr/chronix", "The URL of the Chronix endpoint.")
		maxChunkAge = flag.Duration("max-chunk-age", time.Hour, "The maximum age of a chunk before it is closed and persisted.")
	)
	flag.Parse()

	u, err := url.Parse(*chronixURL)
	if err != nil {
		log.Fatal("Failed to parse Chronix URL:", err)
	}
	chronix := chronix.New(chronix.NewSolrClient(u, nil))

	ing := ingester.NewIngester(
		ingester.Config{
			MaxChunkAge: *maxChunkAge,
		},
		&chronixStore{chronix: chronix},
	)
	defer ing.Stop()
	prometheus.Register(ing)

	http.Handle("/ingest", ingestHandler(ing))
	http.Handle("/metrics", prometheus.Handler())

	log.Println("Listening on", *listenAddr)
	log.Fatalln(http.ListenAndServe(*listenAddr, nil))
}

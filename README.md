# chronix.ingester

The Chronix Ingester batches up sample data from various sources and
stores it as chunks in Chronix. Currently, only [Prometheus](https://prometheus.io/)
is supported.

## Building

Building requires [Go](https://golang.org/dl/) 1.7 or newer, as well as a
working [`GOPATH` setup](https://golang.org/doc/code.html).

To build the ingester:

```
go build
```

## Running

Example:

```
./chronix.ingester -chronix-url=http://my-solr-host:8983/solr/chronix -max-chunk-age=30m
```

To show all flags:

```
./chronix.ingester -h
Usage of ./chronix.ingester:
  -checkpoint-file string
    	The path to the checkpoint file. (default "checkpoint.db")
  -checkpoint-interval duration
    	The interval between checkpoints. (default 5m0s)
  -chronix-commit-within duration
    	The duration after which updates to Chronix should be committed. (default 5s)
  -chronix-url string
    	The URL of the Chronix endpoint. (default "http://localhost:8983/solr/chronix")
  -flush-on-shutdown
    	Whether to flush all chunks to Chronix on shutdown, rather than saving them to a checkpoint. A checkpoint will still be written, but will be empty.
  -listen-addr string
    	The address to listen on. (default ":8080")
  -log.format value
    	Set the log target and format. Example: "logger:syslog?appname=bob&local=7" or "logger:stdout?json=true" (default "logger:stderr")
  -log.level value
    	Only log messages with the given severity or above. Valid levels: [debug, info, warn, error, fatal] (default "info")
  -max-chunk-age duration
    	The maximum age of a chunk before it is closed and persisted. (default 1h0m0s)
```

## Testing

To run tests:

```
go test ./...
```

## Configuring Prometheus

Sending samples to the Chronix ingester from Prometheus requires Prometheus
version 1.2.0 or newer. To configure Prometheus to send samples to the
ingester, add the following stanza to your Prometheus configuration file:

```yml
remote_write:
  url: http://<host>:<port>/ingest
```

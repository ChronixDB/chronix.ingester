# build stage
FROM golang:alpine AS build-env
WORKDIR /go
ADD . src/github.com/ChronixDB/chronix.ingester
WORKDIR ./src/github.com/ChronixDB/chronix.ingester

RUN apk add --no-cache git \
    && go env \
    && go get -v ./... \
    && go build -o chronix.ingester \
    && apk del git
	
# final stage
FROM alpine

RUN apk update \
    && apk add --no-cache ca-certificates

WORKDIR /chronix
COPY --from=build-env /go/src/github.com/ChronixDB/chronix.ingester/chronix.ingester .

EXPOSE 8080/tcp
ENTRYPOINT ["./chronix.ingester"]
CMD ["-h"]

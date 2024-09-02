package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/nireo/dynamo"
)

func main() {
	bindAddr := flag.String("bind", "127.0.0.1:7946", "serf bind address")
	rpcAddr := flag.String("rpc", "127.0.0.1:8000", "rpc address")
	seeds := flag.String("seeds", "", "comma-separated list of seed nodes")
	storePath := flag.String("store", "./dynamo-data", "path to store data on the disk")

	flag.Parse()

	storage, err := dynamo.NewBadgerStore(*storePath)
	if err != nil {
		log.Fatalf("failed to create BadgerDB store: %v", err)
	}
	defer storage.Close()

	config := dynamo.Config{
		N: 3,
		R: 2,
		W: 2,
	}

	seedNodes := []string{}
	if *seeds != "" {
		seedNodes = strings.Split(*seeds, ",")
	}

	node, err := dynamo.NewDynamoNode(config, *bindAddr, seedNodes, *rpcAddr)
	if err != nil {
		log.Fatalf("failed to create DynamoDB node: %v", err)
	}
	defer node.Close()

	node.Storage = storage

	log.Printf("starting dynamo node. bind=%s and rpc=%s", *bindAddr, *rpcAddr)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	log.Printf("shutting down...")
}

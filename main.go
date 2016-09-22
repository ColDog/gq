package main

import (
	"flag"
	"github.com/coldog/go-graph/graph"
	"github.com/coldog/go-graph/server"
	"github.com/coldog/go-graph/store"
	"github.com/coldog/go-graph/store/bigtable"
	"github.com/coldog/go-graph/store/bolt"
	"log"
)

func main() {
	listen := flag.String("listen", ":8231", "listen on")
	db := flag.String("db", "main", "database name")
	backend := flag.String("backend", "bolt", "backend to use (bolt, bigtable)")
	bigtableProject := flag.String("bigtable-project", "", "bigtable project")
	bigtableInstance := flag.String("bigtable-instance", "", "bigtable instance")
	bigtableKeyFile := flag.String("bigtable-key-file", "", "bigtable key file")

	flag.Parse()

	var s store.Store
	if *backend == "bolt" {
		s = bolt.NewBoltStore(*db)
	} else if *backend == "bigtable" {
		s = bigtable.NewBigtableStore(*db, *bigtableProject, *bigtableInstance, *bigtableKeyFile)
	} else {
		log.Fatal("backend not recognized:", backend)
	}

	err := s.Open()
	if err != nil {
		log.Fatal("could not start store: ", err)
	}

	defer s.Close()

	g := graph.New(s)
	serve := server.New(g)

	serve.Serve(*listen)
}

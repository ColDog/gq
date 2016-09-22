package tests

import (
	"github.com/coldog/go-graph/store/bigtable"
	"github.com/coldog/go-graph/store/bolt"
	"testing"
)

func TestBolt(t *testing.T) {
	s := bolt.NewBoltStore("test")
	RunTests(t, s)
}

func TestBigtable(t *testing.T) {
	s := bigtable.NewBigtableStore("test", "rising-coil-143717", "default", "../google-key.json")
	RunTests(t, s)
}

func TestReferenceBigtable(t *testing.T) {
	s := bigtable.NewBigtableStore("test", "rising-coil-143717", "default", "../google-key.json")
	RunTest(t, s, "social-graph:likes-back")
}

package store

import (
	"github.com/coldog/go-graph/objects"
)

type Store interface {

	// Put an object into storage.
	Put(obj ...*objects.Object) error

	// Delete an object from storage.
	Del(obj ...*objects.Object) error

	// Return an object from storage.
	Get(key string) (*objects.Object, error)

	// This method should run a prefix query on the underlying storage, pushing along the provided channel
	// the results returned up to the count variable provided. The channel should be closed when finished.
	Prefix(prefix string, count int) (<-chan *objects.Object, error)

	// A backend may choose to implement a
	Flush()

	// This is a lifecycle hook that can be implemented to set up the database. If an error
	// is returned startup of the program will fail.
	Open() error

	// This is a lifecyle hook that can be implemented by the underlying store. It will be
	// called at program exit.
	Close()

	// Removes the database. Used in testing.
	Drop()

	// Debug is used for testing and should print all of the keys currently in the database
	Debug()
}

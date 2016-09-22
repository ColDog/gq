package bigtable

import (
	"github.com/coldog/go-graph/objects"

	"fmt"
	"github.com/coldog/go-graph/graph"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestBigtable_Basics(t *testing.T) {
	b := &BigTableStore{
		project:   "rising-coil-143717",
		instance:  "default",
		keyFile:   "../../google-key.json",
		tableName: "main",
		lock:      &sync.RWMutex{},
		queue:     make(chan *objects.Object, queueSize),
	}

	err := b.Open()
	ok(t, err)

	defer b.Close()

	println("put")
	err = b.Put(&objects.Object{"test", nil})
	ok(t, err)

	println("get")
	o, err := b.Get("test")
	ok(t, err)

	fmt.Printf("%+v\n", o)

	err = b.Put(&objects.Object{"test_1", nil})
	ok(t, err)

	err = b.Put(&objects.Object{"test_2", nil})
	ok(t, err)

	time.Sleep(1 * time.Second)
	out, err := b.Prefix("", 20)
	ok(t, err)

	for o := range out {
		fmt.Printf("%+v\n", o)
	}

	err = b.Del(&objects.Object{"test", nil})
	ok(t, err)

}

func TestBigtable_Performance(t *testing.T) {
	b := &BigTableStore{
		project:   "rising-coil-143717",
		instance:  "default",
		keyFile:   "../../google-key.json",
		tableName: "main",
		queue:     make(chan *objects.Object, 100),
		lock:      &sync.RWMutex{},
	}

	err := b.Open()
	ok(t, err)
	defer b.Close()
	g := graph.New(b)

	t1 := time.Now()
	for i := 0; i < 50; i++ {
		g.CreateNode("user", nil)
	}
	t2 := time.Now()
	fmt.Println(float64(t2.UnixNano()) - float64(t1.UnixNano()))

	res, err := b.Prefix("user_", 50)
	ok(t, err)

	c := 0
	for o := range res {
		c++
		fmt.Printf("%+v\n", o)
	}
	equals(t, 50, c)
}

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}

package bolt

import (
	"github.com/coldog/go-graph/objects"

	"github.com/boltdb/bolt"

	"bytes"
	"encoding/json"
	"fmt"
	"os"
)

func NewBoltStore(name string) *BoltStore {
	s := &BoltStore{
		name: name,
	}
	return s
}

type BoltStore struct {
	db     *bolt.DB
	bucket []byte
	name   string
}

func (store *BoltStore) Del(objs ...*objects.Object) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		for _, obj := range objs {
			err := tx.Bucket(store.bucket).Delete([]byte(obj.Key))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (store *BoltStore) Put(objs ...*objects.Object) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		for _, obj := range objs {
			err := tx.Bucket(store.bucket).Put([]byte(obj.Key), encode(obj.Val))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (store *BoltStore) Get(key string) (obj *objects.Object, err error) {
	err = store.db.Update(func(tx *bolt.Tx) error {
		data := tx.Bucket(store.bucket).Get([]byte(key))
		if data != nil {
			obj = &objects.Object{Key: key, Val: decode(data)}
		}
		return nil
	})

	return obj, err
}

func (store *BoltStore) Prefix(prefixStr string, count int) (<-chan *objects.Object, error) {
	res := make(chan *objects.Object, count)
	prefix := []byte(prefixStr)

	go func() {
		defer close(res)
		i := 0
		store.db.View(func(tx *bolt.Tx) error {
			c := tx.Bucket(store.bucket).Cursor()
			for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {

				res <- &objects.Object{
					Key: string(k),
					Val: decode(v),
				}

				count--
				if count <= 0 {
					return nil
				}
				i++
			}

			return nil
		})

		// fmt.Println("prefix query:", prefixStr, i)
	}()

	return res, nil
}

func (store *BoltStore) Debug() {
	store.db.View(func(tx *bolt.Tx) error {
		tx.Bucket(store.bucket).ForEach(func(k, v []byte) error {
			fmt.Printf("key=%s, value=%s\n", k, v)
			return nil
		})
		return nil

	})
}

func (store *BoltStore) Close() {
	store.db.Close()
}

func (store *BoltStore) Open() error {
	db, err := bolt.Open(store.name+".db", 0600, nil)
	if err != nil {
		return err
	}

	store.db = db
	store.bucket = []byte("bucket")

	return db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(store.bucket)
		tx.CreateBucketIfNotExists([]byte("sys"))
		return nil
	})
}

func (store *BoltStore) Drop() {
	os.Remove(store.name + ".db")
}

func (store *BoltStore) Flush() {}

func encode(m map[string]interface{}) []byte {
	data, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return data
}

func decode(val []byte) map[string]interface{} {
	m := make(map[string]interface{})
	err := json.Unmarshal(val, &m)
	if err != nil {
		panic(err)
	}
	return m
}

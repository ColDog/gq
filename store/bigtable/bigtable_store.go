package bigtable

import (
	"github.com/coldog/go-graph/objects"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/cloud"
	"google.golang.org/cloud/bigtable"

	"encoding/json"
	"io/ioutil"
	"log"
	"strings"
	"sync"
)

const queueSize = 100
const workers = 20

func NewBigtableStore(table, project, instance, keyFile string) *BigTableStore {
	return &BigTableStore{
		project:   project,
		instance:  instance,
		keyFile:   keyFile,
		tableName: table,
		queue:     make(chan *objects.Object, queueSize),
		lock:      &sync.RWMutex{},
	}
}

type BigTableStore struct {
	project   string
	instance  string
	keyFile   string
	tableName string
	client    *bigtable.Client
	admin     *bigtable.AdminClient
	table     *bigtable.Table
	queue     chan *objects.Object
	lock      *sync.RWMutex
}

func (s *BigTableStore) worker() {
	local := []*objects.Object{}
	for {
		select {
		case obj, ok := <-s.queue:
			if !ok {
				log.Println("[WARN] bigtable-store: worker exiting")
				return
			}

			local = append(local, obj)

			if len(local) >= queueSize {
				log.Println("[DEBUG] bigtable-store: inserting", len(local))
				s.put(local...)
				local = []*objects.Object{}
			}

		default:
			if len(local) > 0 {
				log.Println("[DEBUG] bigtable-store: inserting", len(local))
				s.put(local...)
				local = []*objects.Object{}
			}
		}
	}
}

func (s *BigTableStore) Flush() {
	s.lock.Lock()
	defer s.lock.Unlock()

	log.Println("[DEBUG] bigtable-store: flushing...")
	local := []*objects.Object{}
	for {
		select {
		case obj, ok := <-s.queue:
			if !ok {
				return
			}

			local = append(local, obj)
			if len(local) >= queueSize {
				log.Println("[DEBUG] bigtable-store: flush", len(local))
				s.put(local...)
			}

		default:
			log.Println("[DEBUG] bigtable-store: flush", len(local))
			s.put(local...)
			return
		}
	}
}

func (db *BigTableStore) Open() error {
	ctx := context.Background()

	jsonKey, err := ioutil.ReadFile(db.keyFile)
	if err != nil {
		return err
	}

	adminConfig, err := google.JWTConfigFromJSON(jsonKey, bigtable.AdminScope)
	if err != nil {
		return err
	}

	config, err := google.JWTConfigFromJSON(jsonKey, bigtable.Scope)
	if err != nil {
		return err
	}

	client, err := bigtable.NewClient(ctx, db.project, db.instance, cloud.WithTokenSource(config.TokenSource(ctx)))
	if err != nil {
		return err
	}

	admin, err := bigtable.NewAdminClient(ctx, db.project, db.instance, cloud.WithTokenSource(adminConfig.TokenSource(ctx)))
	if err != nil {
		return err
	}

	_, err = admin.TableInfo(context.Background(), db.tableName)
	if err != nil {
		log.Println("[INFO] bigtable-store: creating table", db.tableName)

		err := admin.CreateTable(context.Background(), db.tableName)
		if err != nil {
			log.Println("[ERROR] bigtable-store: create table failed", err)
			return err
		}

		err = admin.CreateColumnFamily(context.Background(), db.tableName, "body")
		if err != nil {
			log.Println("[ERROR] bigtable-store: column failed", err)
			return err
		}
	}

	db.client = client
	db.admin = admin
	db.table = client.Open(db.tableName)
	for i := 0; i < workers; i++ {
		go db.worker()
	}
	return nil
}

func (db *BigTableStore) put(objs ...*objects.Object) error {
	ctx := context.Background()

	keys := []string{}
	values := []*bigtable.Mutation{}

	for _, obj := range objs {
		mut := bigtable.NewMutation()

		mut.Set("body", "id", bigtable.Now(), []byte(obj.Key))

		for k, v := range obj.Val {
			mut.Set("body", k, bigtable.Now(), encode(v))
		}
		keys = append(keys, obj.Key)
		values = append(values, mut)
	}

	errs, err := db.table.ApplyBulk(ctx, keys, values)
	if err != nil {
		return err
	}

	if len(errs) > 0 {
		return errs[0]
	}

	return nil
}

func (s *BigTableStore) Put(objs ...*objects.Object) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, obj := range objs {
		s.queue <- obj
	}
	return nil
}

func (s *BigTableStore) Del(objs ...*objects.Object) error {

	for _, obj := range objs {
		ctx := context.Background()
		mut := bigtable.NewMutation()
		mut.DeleteRow()
		err := s.table.Apply(ctx, obj.Key, mut)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *BigTableStore) parseRow(r bigtable.Row) *objects.Object {
	if r.Key() == "" {
		return nil
	}

	m := make(map[string]interface{})
	body := r["body"]
	if body != nil {
		for _, item := range body {
			if item.Column != "body:id" {
				m[strings.Split(item.Column, ":")[1]] = decode(item.Value)
			}
		}
	}

	return &objects.Object{r.Key(), m}
}

func (s *BigTableStore) Get(key string) (*objects.Object, error) {
	r, err := s.table.ReadRow(context.Background(), key)
	if err != nil {
		return nil, err
	}

	o := s.parseRow(r)

	return o, nil
}

func (s *BigTableStore) Drop() {
	err := s.admin.DeleteTable(context.Background(), s.tableName)
	if err != nil {
		log.Println("[ERROR] bigtable-store: failed to drop db", err)
	}
}

func (s *BigTableStore) Prefix(prefix string, count int) (<-chan *objects.Object, error) {
	out := make(chan *objects.Object)

	log.Println("[DEBUG] bigtable-store: prefix query", prefix)

	go func() {
		defer close(out)
		err := s.table.ReadRows(context.Background(), bigtable.PrefixRange(prefix), func(r bigtable.Row) bool {
			if r.Key() == "" {
				return true
			}

			o := s.parseRow(r)

			out <- o

			count--
			if count <= 0 {
				return false
			}

			return true
		})

		if err != nil {
			log.Println("[ERROR] bigtable-store: prefix query failed", err)
		}

	}()

	return out, nil
}

func (s *BigTableStore) Close() {
	s.Flush()
}

func (s *BigTableStore) Debug() {}

func encode(m interface{}) []byte {
	data, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return data
}

func decode(val []byte) interface{} {
	var m interface{}
	err := json.Unmarshal(val, &m)
	if err != nil {
		panic(err)
	}
	return m
}

package server

import (
	"github.com/coldog/go-graph/graph"

	"github.com/julienschmidt/httprouter"

	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

func New(g *graph.Graph) *Server {
	return &Server{g}
}

type Server struct {
	g *graph.Graph
}

func (s *Server) traversalQuery(w http.ResponseWriter, r *http.Request, rp httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	t := s.g.Traversal()
	err := json.NewDecoder(r.Body).Decode(t)
	if err != nil {
		w.Write([]byte(`{"error": "JsonErr"}`))
		w.WriteHeader(400)
		return
	}

	res := s.g.Run(t)

	data, err := json.Marshal(map[string]interface{}{
		"results": res,
	})
	if err != nil {
		handleErr(w, 500, err)
		return
	}

	w.Write(data)
}

func (s *Server) pathQuery(w http.ResponseWriter, r *http.Request, rp httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	var id []byte
	if rp.ByName("id") != "" {
		id = []byte(rp.ByName("id"))
	}

	t := s.g.Traversal().Is(rp.ByName("type")).Limit(50).Has("id", id).WithBody()
	if rp.ByName("out") != "" {
		t.Out(rp.ByName("out"))
	}

	res := t.All()

	data, err := json.Marshal(map[string]interface{}{
		"results": res,
	})
	if err != nil {
		handleErr(w, 500, err)
		return
	}

	w.Write(data)
}

func (s *Server) createResource(w http.ResponseWriter, r *http.Request, rp httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	raw, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("[ERROR] server: read err", err)
		w.WriteHeader(400)
		w.Write([]byte(`{"error": "ReadErr"}`))
		return
	}

	body := make(map[string]interface{})
	if raw != nil {
		err = json.Unmarshal(raw, &body)
	}

	if err != nil {
		handleErr(w, 400, err)
		return
	}

	res, err := s.g.PutByResourceID(rp.ByName("id"), body)
	if err != nil {
		handleErr(w, 400, err)
		return
	}

	data, err := json.Marshal(map[string]interface{}{
		"object": res,
	})
	if err != nil {
		handleErr(w, 500, err)
		return
	}

	w.Write(data)
}

func (s *Server) getResource(w http.ResponseWriter, r *http.Request, rp httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	res, err := s.g.GetByResourceID(rp.ByName("id"))
	if err != nil {
		handleErr(w, 400, err)
		return
	}

	data, err := json.Marshal(map[string]interface{}{
		"object": res,
	})
	if err != nil {
		handleErr(w, 500, err)
		return
	}

	w.Write(data)
}

func (s *Server) delResource(w http.ResponseWriter, r *http.Request, rp httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	res, err := s.g.DelByResourceID(rp.ByName("id"))
	if err != nil {
		handleErr(w, 400, err)
		return
	}

	data, err := json.Marshal(map[string]interface{}{
		"object": res,
	})
	if err != nil {
		handleErr(w, 500, err)
		return
	}

	w.Write(data)
}

func (s *Server) Serve(addr string) {
	router := httprouter.New()

	//router.OPTIONS("/*", func(w http.ResponseWriter, r *http.Request, rp httprouter.Params) {
	//	w.Header().Set("Access-Control-Allow-Origin", "*")
	//	w.Header().Set("Access-Control-Allow-Methods", "PUT, GET, POST, DELETE, OPTIONS")
	//	w.Header().Set("Access-Control-Allow-Headers", "Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token, Authorization")
	//})

	router.GET("/", func(w http.ResponseWriter, r *http.Request, rp httprouter.Params) {
		w.Write([]byte("Go-Graph\n"))
	})
	router.GET("/v1/query/nodes/:type", s.pathQuery)
	router.GET("/v1/query/nodes/:type/:id", s.pathQuery)
	router.GET("/v1/query/nodes/:type/:id/:out", s.pathQuery)

	router.POST("/v1/traverse", s.traversalQuery)

	router.PUT("/v1/resources/:id", s.createResource)
	router.GET("/v1/resources/:id", s.getResource)
	router.DELETE("/v1/resources/:id", s.delResource)

	log.Println("[INFO] server: serving", addr)
	log.Fatal(http.ListenAndServe(addr, router))
}

func handleErr(w http.ResponseWriter, status int, err error) {
	log.Println("[ERROR] server: err", err)
	data, jerr := json.Marshal(map[string]interface{} {
		"error": err,
		"message": err.Error(),
	})
	if jerr != nil {
		w.WriteHeader(500)
		w.Write([]byte(`{"error": "JsonErr"}`))
	}

	w.WriteHeader(status)
	w.Write(data)
}
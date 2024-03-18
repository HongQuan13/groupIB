package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Server setup
type Message struct {
	Topic string
	Data  []byte
}

type Config struct{
	ListenAddress string
	StoreCreater StoreCreater 
}

type Server struct {
	*Config
	topics map[string]Storer
	producers []Producer
	mutex     sync.RWMutex
	quitch    chan struct{}
}

// Store Queue
type StoreCreater func() Storer

type Storer interface {
    Push([]byte) (int, error)
	RegisterConsumer(timeout time.Duration) ([]byte, error) 
}

type MemoryStore struct{
	mu	sync.RWMutex
	data [][]byte
	cond    *sync.Cond
	waitChans   []chan []byte  
}

// Producer
type Producer interface {
	Start() error
}

type HTTPProducer struct {
	listenAddress string
	server        *Server 
}

func main() {

	cfg := &Config{
		ListenAddress: "localhost:8000",
		StoreCreater: func() Storer{
			return NewMemmoryStore()
		},
	} 
	s, err := newServer(cfg)
	if err != nil{
		log.Fatal(err)
	}
	s.Start()

}


// Server setup
func newServer(cfg *Config) (*Server, error){
	s := &Server{
        Config: cfg,
        topics: make(map[string]Storer),
        quitch: make(chan struct{}),
    }
	s.producers = []Producer{
        NewHTTPProducer(cfg.ListenAddress, s), 
    }
    return s, nil
}

func (s *Server) Start() {
	for _, producer := range s.producers {
		go func(p Producer) {
			if err := p.Start(); err != nil {
				fmt.Println(err)
			}
		}(producer)
	}
	<-s.quitch
}

func (s *Server) Publish(msg Message) (int,error){
	store := s.GetStoreOfTopic(msg.Topic)
	return store.Push(msg.Data)
}

func (s *Server) GetStoreOfTopic(topic string) Storer{
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, err := s.topics[topic]; !err {
		s.topics[topic] = s.StoreCreater()
		log.Println("Created a new topic", topic)
	}

	return s.topics[topic]
}


// Store Queue
func NewMemmoryStore() *MemoryStore {
    ms := &MemoryStore{
        data: make([][]byte, 0),
		waitChans: make([]chan []byte, 0),
    }
    ms.cond = sync.NewCond(&ms.mu)
    return ms
}

func (s *MemoryStore) Push(b []byte) (int, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

	if len(s.waitChans) > 0 {
		waitChans := make([]chan []byte, len(s.waitChans))
		copy(waitChans, s.waitChans)
		s.waitChans = nil
		for _, waitChan := range waitChans {
			waitChan <- b
			close(waitChan)
		}
    } else {
        s.data = append(s.data, b)
    }
    
    return len(s.data) - 1, nil
}

func (s *MemoryStore) RegisterConsumer(timeout time.Duration) ([]byte, error) {
    s.mu.Lock()
	if len(s.data) > 0 {
        msg := s.data[0]
        s.data = s.data[1:] 
        s.mu.Unlock()
        return msg, nil
    }

    waitChan := make(chan []byte, 1)
    s.waitChans = append(s.waitChans, waitChan)

    s.mu.Unlock()

    select {
    case msg := <-waitChan:
        return msg, nil
    case <-time.After(timeout):
        s.mu.Lock()
        for i, ch := range s.waitChans {
            if ch == waitChan {
                s.waitChans = append(s.waitChans[:i], s.waitChans[i+1:]...)
                break
            }
        }
        s.mu.Unlock()
        return nil, fmt.Errorf("timeout")
    }
}


// HttpProducer
func NewHTTPProducer(listenAddress string, server *Server) *HTTPProducer {
	return &HTTPProducer{
		listenAddress: listenAddress,
		server: server,
	}
}

func (p *HTTPProducer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    path := strings.TrimPrefix(r.URL.Path, "/")
    parts := strings.Split(path, "/")
    queryParams := r.URL.Query()

    if len(parts) != 1 || parts[0] == "" {
        http.Error(w, "Queue name is required", http.StatusBadRequest)
        return
    }
    queueName := parts[0]

    switch r.Method {
    case "GET":
        timeoutStr := queryParams.Get("timeout")
        timeout, err := strconv.Atoi(timeoutStr)
        if err != nil {
            http.Error(w, "Invalid timeout value", http.StatusBadRequest)
            return
        }

        store := p.server.GetStoreOfTopic(queueName) 
        msg, err := store.RegisterConsumer(time.Duration(timeout) * time.Second)
        if err != nil {
            http.Error(w, "Not Found", http.StatusNotFound)
            return
        }

        w.WriteHeader(http.StatusOK)
        w.Write(msg)

    case "PUT":
        messageValue := queryParams.Get("v")
        if messageValue == "" {
            http.Error(w, "Parameter 'v' is required", http.StatusBadRequest)
            return
        }

        store := p.server.GetStoreOfTopic(queueName)
        _, err := store.Push([]byte(messageValue))
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }

        w.WriteHeader(http.StatusOK)
        fmt.Fprint(w, "OK")

    default:
        http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
    }
}

func (p *HTTPProducer) Start() error {
	log.Println("HTTP running", "port", p.listenAddress)
	return http.ListenAndServe(p.listenAddress, p)
}




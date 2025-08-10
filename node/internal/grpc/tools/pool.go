package tools

import (
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type Pool struct {
	conns map[string]*grpc.ClientConn
	mu    sync.RWMutex
}

func (p *Pool) For(url string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	conn, ok := p.conns[url]
	p.mu.RUnlock()

	if ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, exists := p.conns[url]; exists {
		return conn, nil
	}

	p.conns[url] = conn
	return conn, nil
}

func NewPool() *Pool {
	p := &Pool{
		conns: make(map[string]*grpc.ClientConn),
	}

	go p.monitorConnections()

	return p
}

// Периодически проверяет состояние соединений
func (p *Pool) monitorConnections() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.checkConnections()
		}
	}
}

// Проверяет, живы ли соединения
func (p *Pool) checkConnections() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for url, conn := range p.conns {
		state := conn.GetState()
		if state != connectivity.Ready && state != connectivity.Idle && state != connectivity.Connecting {
			conn.Close()
			delete(p.conns, url)
		}
	}
}

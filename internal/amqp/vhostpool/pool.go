package pool

import (
	"fmt"
	"sync"

	"github.com/Space-DF/broker-bridge-service/internal/amqp/helpers"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Pool struct {
	mu          sync.Mutex
	brokerURL   string
	connections map[string]*pooledConnection
}

type pooledConnection struct {
	conn     *amqp.Connection
	refCount int
}

func New(brokerURL string) *Pool {
	return &Pool{
		brokerURL:   brokerURL,
		connections: make(map[string]*pooledConnection),
	}
}

func isConnClosed(conn *amqp.Connection) bool {
	return conn == nil || conn.IsClosed()
}

func (p *Pool) Acquire(vhost string) (*amqp.Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if pooled, ok := p.connections[vhost]; ok {
		// Check if connection is still valid
		if !isConnClosed(pooled.conn) {
			pooled.refCount++
			return pooled.conn, nil
		}
		// Connection is closed, remove it and create a new one
		delete(p.connections, vhost)
	}

	url, err := helpers.BuildVhostURL(vhost, p.brokerURL)
	if err != nil {
		return nil, err
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("connect to vhost %s: %w", vhost, err)
	}

	p.connections[vhost] = &pooledConnection{conn: conn, refCount: 1}
	return conn, nil
}

func (p *Pool) Release(vhost string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pooled, ok := p.connections[vhost]
	if !ok || pooled == nil {
		return
	}
	pooled.refCount--
	if pooled.refCount <= 0 {
		_ = pooled.conn.Close()
		delete(p.connections, vhost)
	}
}

func (p *Pool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for vhost, pooled := range p.connections {
		if pooled != nil && pooled.conn != nil {
			_ = pooled.conn.Close()
		}
		delete(p.connections, vhost)
	}
}

// InvalidateAll clears all cached connections without closing them
// Use when connections are known to be closed (e.g., after broker restart)
func (p *Pool) InvalidateAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connections = make(map[string]*pooledConnection)
}

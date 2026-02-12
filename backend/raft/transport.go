package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type VirtualTransport struct {
	mu sync.Mutex
	// Map of NodeID -> Channel for incoming messages
	inboxes map[int]chan Message
	
	// Chaos settings
	dropRate float64
	latency  time.Duration
}

func NewVirtualTransport() *VirtualTransport {
	return &VirtualTransport{
		inboxes:  make(map[int]chan Message),
		dropRate: 0.0,
		latency:  0,
	}
}

func (vt *VirtualTransport) Register(nodeID int) chan Message {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	ch := make(chan Message, 100)
	vt.inboxes[nodeID] = ch
	return ch
}

func (vt *VirtualTransport) Send(msg Message) {
	// Chaos Logic
	vt.mu.Lock()
	if rand.Float64() < vt.dropRate {
		vt.mu.Unlock()
		fmt.Printf("Dropped message from %d to %d\n", msg.From, msg.To)
		return
	}
	latency := vt.latency
	inbox, ok := vt.inboxes[msg.To]
	vt.mu.Unlock()

	if !ok {
		return // Node might be disconnected or not registered
	}

	go func() {
		if latency > 0 {
			time.Sleep(latency)
		}
		select {
		case inbox <- msg:
		default:
			fmt.Printf("Inbox full for node %d, dropping message\n", msg.To)
		}
	}()
}

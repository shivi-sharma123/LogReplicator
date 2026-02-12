package raft

import (
	"sync"
	"time"
)

type ClusterController struct {
	mu        sync.Mutex
	Nodes     map[int]*RaftNode
	Transport *VirtualTransport
}

func NewClusterController(nodeCount int) *ClusterController {
	transport := NewVirtualTransport()
	nodes := make(map[int]*RaftNode)

	// Create IDs 1..N
	var allIDs []int
	for i := 1; i <= nodeCount; i++ {
		allIDs = append(allIDs, i)
	}

	for i := 1; i <= nodeCount; i++ {
		// Peers are everyone else
		var peers []int
		for _, id := range allIDs {
			if id != i {
				peers = append(peers, id)
			}
		}
		nodes[i] = NewRaftNode(i, peers, transport)
	}

	return &ClusterController{
		Nodes:     nodes,
		Transport: transport,
	}
}

func (cc *ClusterController) Start() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	for _, node := range cc.Nodes {
		node.Start()
	}
}

func (cc *ClusterController) StopNode(id int) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if node, ok := cc.Nodes[id]; ok {
		node.Stop()
	}
}

func (cc *ClusterController) StartNode(id int) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if node, ok := cc.Nodes[id]; ok {
		node.Resume()
	}
}

// GetState returns a serializable snapshot of the cluster
func (cc *ClusterController) GetState() interface{} {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	type NodeState struct {
		ID          int        `json:"id"`
		State       string     `json:"state"`
		CurrentTerm int        `json:"current_term"`
		VotedFor    int        `json:"voted_for"`
		CommitIndex int        `json:"commit_index"`
		IsAlive     bool       `json:"is_alive"`
		Log         []LogEntry `json:"log"`
	}

	var states []NodeState
	// Sort by ID for consistent output? Iterating map is random.
	// Let's loop 1..len
	for i := 1; i <= len(cc.Nodes); i++ {
		if node, ok := cc.Nodes[i]; ok {
			info := node.GetState()
			// We can pass the NodeInfo struct directly to JSON if we want,
			// but we are mapping it to NodeState (defined inside GetState in controller... wait, I removed NodeState struct definition in previous edits?
			// Checking previous file content...
			// I see I'm appending to `states` which is `[]NodeState`. `NodeState` is defined inside `GetState`.
			// I need to update `NodeState` struct inside `GetState` as well.

			// Actually, let's just use NodeInfo directly if we can, or update the local struct.
			// To be safe and clean, I'll update the local struct in the controller.
			states = append(states, NodeState{
				ID:          info.ID,
				State:       string(info.State),
				CurrentTerm: info.CurrentTerm,
				VotedFor:    info.VotedFor,
				CommitIndex: info.CommitIndex,
				IsAlive:     info.IsAlive,
				Log:         info.Log,
			})
		}
	}

	return map[string]interface{}{
		"nodes":     states,
		"timestamp": time.Now().UnixMilli(),
	}
}

func (cc *ClusterController) Submit(cmd string) bool {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	// Find leader
	for _, node := range cc.Nodes {
		// This is racy if we check IsLeader property, but SubmitCommand checks inside.
		// However, we need to iterate and try submitting.
		// node.SubmitCommand locks the node.
		if node.SubmitCommand(cmd) {
			return true
		}
	}
	return false
}

package main

import (
	"raft-simulator/api"
	"raft-simulator/raft"
)

func main() {
	// Create cluster with 5 nodes
	controller := raft.NewClusterController(5)

	// Start simulation
	controller.Start()

	// Start API Server
	server := api.NewServer(controller)
	server.Run(":8080")
}

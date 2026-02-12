package api

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"raft-simulator/raft"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type Server struct {
	Controller *raft.ClusterController
}

func NewServer(controller *raft.ClusterController) *Server {
	return &Server{
		Controller: controller,
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all CORS for this simulation
	},
}

func (s *Server) Run(addr string) {
	r := gin.Default()

	// CORS Middleware
	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	api := r.Group("/control")
	{
		api.POST("/stop/:id", s.handleStopNode)
		api.POST("/start/:id", s.handleStartNode)
		api.POST("/submit", s.handleSubmit)
	}

	r.GET("/ws", s.handleWebSocket)

	log.Printf("🚀 Server listening on http://localhost%s", addr)
	r.Run(addr)
}

func (s *Server) handleStopNode(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID"})
		return
	}
	s.Controller.StopNode(id)
	c.JSON(http.StatusOK, gin.H{"status": "stopped", "id": id})
}

func (s *Server) handleStartNode(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID"})
		return
	}
	s.Controller.StartNode(id)
	c.JSON(http.StatusOK, gin.H{"status": "started", "id": id})
}

type SubmitRequest struct {
	Command string `json:"command"`
}

func (s *Server) handleSubmit(c *gin.Context) {
	var req SubmitRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid Request"})
		return
	}

	success := s.Controller.Submit(req.Command)
	if success {
		c.JSON(http.StatusOK, gin.H{"status": "submitted"})
	} else {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "No leader available"})
	}
}

func (s *Server) handleWebSocket(c *gin.Context) {
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("WS Upgrade error:", err)
		return
	}
	defer ws.Close()

	// Start a goroutine to read from the socket (drain input to handle Pings/Close)
	go func() {
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// Push state every 100ms
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			state := s.Controller.GetState()
			if err := ws.WriteJSON(state); err != nil {
				log.Println("WS Write error:", err)
				return
			}
		}
	}
}

.PHONY: run-backend run-frontend run stop clean

# Default port for backend
BACKEND_PORT := 8080
# Default port for frontend (Vite default)
FRONTEND_PORT := 5173

run-backend:
	@echo "🚀 Starting Backend..."
	@echo "   - API: http://localhost:$(BACKEND_PORT)/control"
	@echo "   - WebSocket: ws://localhost:$(BACKEND_PORT)/ws"
	cd backend && go run main.go

run-frontend:
	@echo "🚀 Starting Frontend..."
	@echo "   - URL: http://localhost:$(FRONTEND_PORT)"
	cd frontend && npm run dev

run:
	@echo "🚀 Starting Raft Simulator System..."
	@make -j 2 run-backend run-frontend

stop:
	@echo "🛑 Stopping servers..."
	@# Kill Go backend
	-pkill -f "go run main.go" || true
	-pkill -f "raft-backend" || true
	-lsof -ti:$(BACKEND_PORT) | xargs kill -9 2>/dev/null || true
	@# Kill Node frontend
	-pkill -f "vite" || true
	-lsof -ti:$(FRONTEND_PORT) | xargs kill -9 2>/dev/null || true
	@echo "✅ Servers stopped."

clean:
	@echo "🧹 Cleaning..."
	rm -f backend/raft-backend

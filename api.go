package main

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Server HTTP/WS handler'larni va bog'liqliklarni ushlaydi.
type Server struct {
	cfg      Config
	hub      *Hub
	store    *TaskStore
	upgrader websocket.Upgrader
}

func NewServer(cfg Config, hub *Hub, store *TaskStore) *Server {
	return &Server{
		cfg:   cfg,
		hub:   hub,
		store: store,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     func(*http.Request) bool { return true },
		},
	}
}

// Routes barcha endpoint'larni ro'yxatdan o'tkazadi (Go 1.22+ ServeMux pattern'lari).
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", s.handleWS)
	mux.HandleFunc("POST /tasks", s.auth(s.handleCreateTask))
	mux.HandleFunc("GET /tasks/{id}", s.auth(s.handleGetTask))
	mux.HandleFunc("GET /healthz", s.auth(s.handleHealth))
	return mux
}

// auth bearer token'ni tekshiruvchi middleware.
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.validToken(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "ruxsat yo'q"})
			return
		}
		next(w, r)
	}
}

// validToken Authorization: Bearer <token> yoki ?token= ni tekshiradi.
func (s *Server) validToken(r *http.Request) bool {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		if strings.TrimPrefix(h, "Bearer ") == s.cfg.AuthToken {
			return true
		}
	}
	return r.URL.Query().Get("token") == s.cfg.AuthToken
}

// handleWS worker WebSocket ulanishini qabul qiladi.
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	if !s.validToken(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "ruxsat yo'q"})
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade o'zi javob yozadi
	}
	client := NewClient(uuid.NewString(), s.hub, conn)
	s.hub.register <- client
	go client.writePump()
	go client.readPump()
}

// createTaskRequest POST /tasks body'si.
type createTaskRequest struct {
	Payload json.RawMessage `json:"payload"`
}

// handleCreateTask yangi task yaratib client'ga yuboradi (sync yoki async).
func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "noto'g'ri JSON"})
			return
		}
	}

	task := s.store.Create(req.Payload)

	// Umuman active worker yo'q bo'lsa darrov no_worker (503).
	if !s.hub.HasActiveClient() {
		s.store.Finalize(task.ID, StatusNoWorker, nil, "active client yo'q")
		t, _ := s.store.Get(task.ID)
		writeJSON(w, http.StatusServiceUnavailable, t)
		return
	}

	// Dispatch fonda ishlaydi: timeout/uzilishda boshqa worker'ga retry qiladi.
	go s.hub.Dispatch(task)

	// Sync rejim: ?wait=true bo'lsa yakuniy javobni (barcha retry'lardan keyin) kutamiz.
	if r.URL.Query().Get("wait") == "true" {
		if ok := s.store.Wait(task, s.cfg.WaitTimeout); !ok {
			t, _ := s.store.Get(task.ID)
			writeJSON(w, http.StatusGatewayTimeout, t) // task baribir GET orqali qoladi
			return
		}
		t, _ := s.store.Get(task.ID)
		status := http.StatusOK
		if t.Status != StatusDone {
			status = http.StatusBadGateway // failed/no_worker
		}
		writeJSON(w, status, t)
		return
	}

	// Async rejim: darhol task_id qaytaramiz.
	t, _ := s.store.Get(task.ID)
	writeJSON(w, http.StatusAccepted, t)
}

// handleGetTask task status va javobini id bo'yicha qaytaradi.
func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	t, ok := s.store.Get(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "task topilmadi"})
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// handleHealth active client va task statistikasini qaytaradi.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	total, byStatus := s.store.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"active_clients": s.hub.ActiveCount(),
		"tasks_total":    total,
		"tasks_by_status": byStatus,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

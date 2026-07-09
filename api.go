package main

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// multipartOverhead — /files upload uchun MaxFileSize ustiga qo'shiladigan zaxira
// (multipart chegaralari va boshqa maydonlar uchun).
const multipartOverhead = 1 << 20 // 1 MiB

// Server HTTP/WS handler'larni va bog'liqliklarni ushlaydi.
type Server struct {
	cfg      Config
	hub      *Hub
	store    *TaskStore
	blobs    *BlobStore
	upgrader websocket.Upgrader
}

func NewServer(cfg Config, hub *Hub, store *TaskStore, blobs *BlobStore) *Server {
	return &Server{
		cfg:   cfg,
		hub:   hub,
		store: store,
		blobs: blobs,
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
	mux.HandleFunc("GET /clients", s.auth(s.handleClients))
	mux.HandleFunc("GET /healthz", s.auth(s.handleHealth))
	// Fayl endpoint'lari ikkala tomon uchun: tashqi client (upload/download) va
	// worker (kirishni oladi, natijani yuklaydi) — shu bois authAny.
	mux.HandleFunc("POST /files", s.authAny(s.handleUpload))
	mux.HandleFunc("GET /files/{id}", s.authAny(s.handleDownload))
	return mux
}

// auth API token'ini tekshiruvchi middleware (task yuborish/o'qish uchun).
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !tokenMatches(r, s.cfg.APIToken) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "ruxsat yo'q"})
			return
		}
		next(w, r)
	}
}

// authAny API yoki WORKER token'ini qabul qiladi (fayl endpoint'lari ikkala
// tomon — tashqi client va worker — tomonidan ishlatiladi).
func (s *Server) authAny(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !tokenMatches(r, s.cfg.APIToken) && !tokenMatches(r, s.cfg.WorkerToken) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "ruxsat yo'q"})
			return
		}
		next(w, r)
	}
}

// tokenMatches Authorization: Bearer <token> yoki ?token= ni kutilgan token bilan solishtiradi.
// Solishtirish constant-time (timing-attack'ning oldini oladi).
func tokenMatches(r *http.Request, want string) bool {
	if want == "" {
		return false
	}
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		if secureEqual(strings.TrimPrefix(h, "Bearer "), want) {
			return true
		}
	}
	return secureEqual(r.URL.Query().Get("token"), want)
}

// secureEqual ikki tokenni constant-time solishtiradi (uzunlik farqi tez rad etiladi).
func secureEqual(got, want string) bool {
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// handleWS worker WebSocket ulanishini qabul qiladi (WORKER_TOKEN bilan).
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	if !tokenMatches(r, s.cfg.WorkerToken) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "ruxsat yo'q"})
		return
	}
	ip := clientIP(r)
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade o'zi javob yozadi
	}
	client := NewClient(uuid.NewString(), ip, s.hub, conn)
	log.Printf("worker ulanmoqda: id=%s ip=%s", client.id, ip)
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

// handleClients ulangan worker'lar ro'yxatini (id, ip, ulangan vaqt) qaytaradi.
func (s *Server) handleClients(w http.ResponseWriter, r *http.Request) {
	clients := s.hub.Clients()
	writeJSON(w, http.StatusOK, map[string]any{
		"count":   len(clients),
		"clients": clients,
	})
}

// handleHealth active client va task statistikasini qaytaradi.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	total, byStatus := s.store.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"active_clients":  s.hub.ActiveCount(),
		"tasks_total":     total,
		"tasks_by_status": byStatus,
	})
}

// handleUpload multipart/form-data'dagi "file" maydonini omborga stream qilib
// yozadi va file_id/metama'lumotni qaytaradi. Task payload'iga baytlar emas,
// shu file_id uzatiladi.
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	// Limitdan katta body'ni multipart uni to'liq spool qilmasidan oldin to'xtatamiz.
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxFileSize+multipartOverhead)
	file, header, err := r.FormFile("file")
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "fayl hajmi limitdan oshib ketdi"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "\"file\" maydoni topilmadi"})
		return
	}
	defer file.Close()

	meta, err := s.blobs.Put(header.Filename, header.Header.Get("Content-Type"), file)
	if err != nil {
		if errors.Is(err, ErrBlobTooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "fayl hajmi limitdan oshib ketdi"})
			return
		}
		log.Printf("fayl saqlash xatosi: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "faylni saqlab bo'lmadi"})
		return
	}
	writeJSON(w, http.StatusCreated, meta)
}

// handleDownload file_id bo'yicha faylni stream qilib qaytaradi.
func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	meta, f, err := s.blobs.Open(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "fayl topilmadi"})
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	if meta.Filename != "" {
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", meta.Filename))
	}
	io.Copy(w, f)
}

// clientIP so'rovning haqiqiy IP manzilini aniqlaydi (proxy header'larini hisobga olib).
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	if xr := r.Header.Get("X-Real-IP"); xr != "" {
		return strings.TrimSpace(xr)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

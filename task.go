package main

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Task status'lari.
const (
	StatusPending    = "pending"    // yaratildi, hali yuborilmadi
	StatusDispatched = "dispatched" // client'ga yuborildi, javob kutilmoqda
	StatusDone       = "done"       // client muvaffaqiyatli javob qaytardi
	StatusFailed     = "failed"     // xatolik (barcha urinishlar muvaffaqiyatsiz)
	StatusNoWorker   = "no_worker"  // active client topilmadi
)

// taskEvent — dispatcher goroutine'ga yuboriladigan signal (worker javobi yoki uzilishi).
// Dispatcher shu signallarga qarab task'ni yakunlaydi yoki boshqa worker'ga retry qiladi.
type taskEvent struct {
	clientID   string          // qaysi client'dan kelgani (eski urinishlarni filtrlash uchun)
	status     string          // StatusDone / StatusFailed (worker bergan)
	data       json.RawMessage // muvaffaqiyatli natija
	errMsg     string          // worker xatosi
	disconnect bool            // client uzilib qoldi
}

// Attempt — bitta worker'ga urinishning natijasi (tarix uchun).
type Attempt struct {
	Num      int    `json:"num"`             // urinish raqami (1, 2, ...)
	ClientID string `json:"client_id"`       // qaysi worker
	ClientIP string `json:"client_ip"`       // worker IP'si
	Outcome  string `json:"outcome"`         // done / timeout / failed / disconnect / send_failed
	Error    string `json:"error,omitempty"` // muvaffaqiyatsizlik sababi
}

// Task bitta so'rovni ifodalaydi.
type Task struct {
	ID         string          `json:"task_id"`
	Status     string          `json:"status"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	Error      string          `json:"error,omitempty"`
	ClientID   string          `json:"client_id,omitempty"`   // taskni bajargan (yoki joriy) worker
	ClientIP   string          `json:"client_ip,omitempty"`   // shu worker IP'si
	Attempts   int             `json:"attempts"`              // nechta worker'ga urinilgani
	AttemptLog []Attempt       `json:"attempt_log,omitempty"` // har urinish tarixi
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`

	done   chan struct{}  // sync waiter'lar (?wait=true) uchun; YAKUNIY holatda close qilinadi
	events chan taskEvent // dispatcher uchun ichki signal kanali
}

// TaskStore — in-memory task reyestri (goroutine-safe).
type TaskStore struct {
	mu    sync.RWMutex
	tasks map[string]*Task
}

func NewTaskStore() *TaskStore {
	return &TaskStore{tasks: make(map[string]*Task)}
}

// Create yangi task yaratadi va saqlaydi.
func (s *TaskStore) Create(payload json.RawMessage) *Task {
	now := time.Now()
	t := &Task{
		ID:        uuid.NewString(),
		Status:    StatusPending,
		Payload:   payload,
		CreatedAt: now,
		UpdatedAt: now,
		done:      make(chan struct{}),
		events:    make(chan taskEvent, 8),
	}
	s.mu.Lock()
	s.tasks[t.ID] = t
	s.mu.Unlock()
	return t
}

// Get task'ni id bo'yicha qaytaradi.
func (s *TaskStore) Get(id string) (*Task, bool) {
	s.mu.RLock()
	t, ok := s.tasks[id]
	s.mu.RUnlock()
	return t, ok
}

// isTerminal status yakuniymi (boshqa o'zgartirib bo'lmaydi).
func isTerminal(status string) bool {
	return status == StatusDone || status == StatusFailed || status == StatusNoWorker
}

// MarkDispatched task'ni navbatdagi worker'ga yuborilgan deb belgilaydi (har urinishda).
func (s *TaskStore) MarkDispatched(id, clientID, clientIP string, attempt int) {
	s.mu.Lock()
	t, ok := s.tasks[id]
	if ok && !isTerminal(t.Status) {
		t.Status = StatusDispatched
		t.ClientID = clientID
		t.ClientIP = clientIP
		t.Attempts = attempt
		t.UpdatedAt = time.Now()
	}
	s.mu.Unlock()
}

// AddAttempt urinish natijasini task tarixiga qo'shadi.
func (s *TaskStore) AddAttempt(id string, a Attempt) {
	s.mu.Lock()
	if t, ok := s.tasks[id]; ok && !isTerminal(t.Status) {
		t.AttemptLog = append(t.AttemptLog, a)
		t.Attempts = len(t.AttemptLog)
	}
	s.mu.Unlock()
}

// Finalize task'ni YAKUNIY holatga keltiradi (faqat dispatcher chaqiradi).
// Birinchi finalize g'olib — keyingilari e'tiborsiz qoladi.
func (s *TaskStore) Finalize(id, status string, result json.RawMessage, errMsg string) {
	s.mu.Lock()
	t, ok := s.tasks[id]
	if !ok || isTerminal(t.Status) {
		s.mu.Unlock()
		return
	}
	t.Status = status
	t.Result = result
	t.Error = errMsg
	t.UpdatedAt = time.Now()
	s.mu.Unlock()

	close(t.done) // sync waiter'larni uyg'otamiz
}

// emit task'ga signal yuboradi (handleResult va disconnect chaqiradi). Bloklamaydi.
func (s *TaskStore) emit(id string, ev taskEvent) {
	s.mu.RLock()
	t, ok := s.tasks[id]
	s.mu.RUnlock()
	if !ok {
		return
	}
	select {
	case t.events <- ev:
	default: // buffer to'la — dispatcher baribir timeout bilan davom etadi
	}
}

// EmitDisconnect uzilgan client'ning ochiq task'lariga disconnect signali yuboradi.
func (s *TaskStore) EmitDisconnect(clientID string) {
	s.mu.RLock()
	var ids []string
	for id, t := range s.tasks {
		if t.ClientID == clientID && t.Status == StatusDispatched {
			ids = append(ids, id)
		}
	}
	s.mu.RUnlock()
	for _, id := range ids {
		s.emit(id, taskEvent{clientID: clientID, disconnect: true})
	}
}

// Wait sync requester uchun task YAKUNLANISHINI (yoki timeout) kutadi.
func (s *TaskStore) Wait(t *Task, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-t.done:
		return true
	case <-timer.C:
		return false
	}
}

// GC ttl'dan eski YAKUNLANGAN task'larni o'chiradi (in-flight task'larga tegmaydi).
func (s *TaskStore) GC(ttl time.Duration) {
	cutoff := time.Now().Add(-ttl)
	s.mu.Lock()
	for id, t := range s.tasks {
		if isTerminal(t.Status) && t.UpdatedAt.Before(cutoff) {
			delete(s.tasks, id)
		}
	}
	s.mu.Unlock()
}

// RunGC har interval'da GC ishga tushiradi (goroutine'da chaqiriladi).
func (s *TaskStore) RunGC(interval, ttl time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		s.GC(ttl)
	}
}

// Stats jami va status bo'yicha task sonini qaytaradi.
func (s *TaskStore) Stats() (total int, byStatus map[string]int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byStatus = make(map[string]int)
	for _, t := range s.tasks {
		byStatus[t.Status]++
	}
	return len(s.tasks), byStatus
}

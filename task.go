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
	StatusFailed     = "failed"     // xatolik (client xatosi yoki uzilish)
	StatusNoWorker   = "no_worker"  // active client topilmadi
)

// Task bitta so'rovni ifodalaydi.
type Task struct {
	ID        string          `json:"task_id"`
	Status    string          `json:"status"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     string          `json:"error,omitempty"`
	ClientID  string          `json:"client_id,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`

	done chan struct{} // sync waiter'lar (?wait=true) uchun; tugaganda close qilinadi
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

// setStatus task statusini yangilaydi (terminal bo'lsa waiter'larni uyg'otadi).
func (s *TaskStore) setStatus(id, status, clientID string, result json.RawMessage, errMsg string) {
	s.mu.Lock()
	t, ok := s.tasks[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	t.Status = status
	t.UpdatedAt = time.Now()
	if clientID != "" {
		t.ClientID = clientID
	}
	if result != nil {
		t.Result = result
	}
	if errMsg != "" {
		t.Error = errMsg
	}
	isTerminal := status == StatusDone || status == StatusFailed || status == StatusNoWorker
	s.mu.Unlock()

	if isTerminal {
		s.closeDone(t)
	}
}

// closeDone done channel'ni faqat bir marta yopadi.
func (s *TaskStore) closeDone(t *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-t.done:
		// allaqachon yopilgan
	default:
		close(t.done)
	}
}

// MarkDispatched task'ni client'ga yuborilgan deb belgilaydi.
func (s *TaskStore) MarkDispatched(id, clientID string) {
	s.setStatus(id, StatusDispatched, clientID, nil, "")
}

// Complete client javobi bilan task'ni yakunlaydi.
func (s *TaskStore) Complete(id, status string, result json.RawMessage, errMsg string) {
	s.setStatus(id, status, "", result, errMsg)
}

// Wait sync requester uchun task tugashini (yoki timeout) kutadi.
// Tugasa true, timeout bo'lsa false qaytaradi.
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

// FailTasksForClient client uzilganda uning ochiq task'larini fail qiladi.
func (s *TaskStore) FailTasksForClient(clientID string) {
	s.mu.RLock()
	var ids []string
	for id, t := range s.tasks {
		if t.ClientID == clientID && t.Status == StatusDispatched {
			ids = append(ids, id)
		}
	}
	s.mu.RUnlock()
	for _, id := range ids {
		s.Complete(id, StatusFailed, nil, "worker_disconnected")
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

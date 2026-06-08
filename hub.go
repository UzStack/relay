package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"
)

// outMessage — server'dan client'ga yuboriladigan task xabari.
type outMessage struct {
	Type    string          `json:"type"`
	TaskID  string          `json:"task_id"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// inMessage — client'dan keladigan natija xabari.
type inMessage struct {
	Type   string          `json:"type"`
	TaskID string          `json:"task_id"`
	Status string          `json:"status"`
	Data   json.RawMessage `json:"data,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// Hub barcha ulangan client'larni boshqaradi va task'larni round-robin tarqatadi.
type Hub struct {
	cfg   Config
	store *TaskStore

	mu      sync.Mutex
	clients map[string]*Client
	order   []string // round-robin tartibi (client_id'lar)
	rrIndex int

	register   chan *Client
	unregister chan *Client
}

func NewHub(cfg Config, store *TaskStore) *Hub {
	return &Hub{
		cfg:        cfg,
		store:      store,
		clients:    make(map[string]*Client),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// Run hub'ning asosiy event-loop'i (bitta goroutine).
func (h *Hub) Run() {
	for {
		select {
		case c := <-h.register:
			h.mu.Lock()
			h.clients[c.id] = c
			h.order = append(h.order, c.id)
			n := len(h.clients)
			h.mu.Unlock()
			log.Printf("client ulandi: %s (jami active: %d)", c.id, n)

		case c := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[c.id]; ok {
				delete(h.clients, c.id)
				h.removeFromOrder(c.id)
				close(c.send)
			}
			n := len(h.clients)
			h.mu.Unlock()
			log.Printf("client uzildi: %s (jami active: %d)", c.id, n)
			// Ochiq task'larga disconnect signali — dispatcher boshqa worker'ga retry qiladi.
			h.store.EmitDisconnect(c.id)
		}
	}
}

// removeFromOrder order slice'idan client_id'ni olib tashlaydi (mu ushlab turilgan holda).
func (h *Hub) removeFromOrder(id string) {
	for i, cid := range h.order {
		if cid == id {
			h.order = append(h.order[:i], h.order[i+1:]...)
			return
		}
	}
}

// HasActiveClient hech bo'lmaganda bitta active client borligini bildiradi.
func (h *Hub) HasActiveClient() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients) > 0
}

// ActiveCount active client sonini qaytaradi.
func (h *Hub) ActiveCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

// nextClientExcluding round-robin bo'yicha keyingi, hali urinilmagan active client'ni qaytaradi.
func (h *Hub) nextClientExcluding(tried map[string]bool) *Client {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := len(h.order)
	for i := 0; i < n; i++ {
		h.rrIndex = h.rrIndex % len(h.order)
		id := h.order[h.rrIndex]
		h.rrIndex++
		if !tried[id] {
			return h.clients[id]
		}
	}
	return nil
}

// sendTask task'ni client'ning send buffer'iga yozadi (bloklamaydi).
func (h *Hub) sendTask(c *Client, t *Task) bool {
	data, err := json.Marshal(outMessage{Type: "task", TaskID: t.ID, Payload: t.Payload})
	if err != nil {
		return false
	}
	select {
	case c.send <- data:
		return true
	default:
		return false // client band / send buffer to'la
	}
}

// urinish natijasi turlari.
type outcomeKind int

const (
	outcomeDone outcomeKind = iota
	outcomeFailed
	outcomeDisconnect
	outcomeTimeout
)

type outcome struct {
	kind   outcomeKind
	data   json.RawMessage
	reason string
}

// Dispatch task'ni active worker'ga yuboradi; javob kelmasa/uzilsa BOSHQA worker'ga
// MaxRetries martagacha qayta yuboradi. Bu funksiya bloklaydi — goroutine'da chaqiriladi.
func (h *Hub) Dispatch(t *Task) {
	tried := make(map[string]bool)
	maxAttempts := h.cfg.MaxRetries + 1

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		c := h.nextClientExcluding(tried)
		if c == nil {
			// urinib ko'rilmagan worker qolmadi
			if attempt == 1 {
				h.store.Finalize(t.ID, StatusNoWorker, nil, "active client yo'q")
			} else {
				h.store.Finalize(t.ID, StatusFailed, nil, "barcha worker'lar urinib ko'rildi")
			}
			return
		}
		tried[c.id] = true

		if !h.sendTask(c, t) {
			continue // bu worker'ga yuborib bo'lmadi, keyingisini sinaymiz
		}
		h.store.MarkDispatched(t.ID, c.id, attempt)

		out := h.waitOutcome(t, c.id)
		if out.kind == outcomeDone {
			h.store.Finalize(t.ID, StatusDone, out.data, "")
			return
		}
		log.Printf("task %s urinish %d/%d (worker %s) muvaffaqiyatsiz: %s",
			t.ID, attempt, maxAttempts, c.id, out.reason)
		// boshqa worker bilan davom etamiz
	}

	h.store.Finalize(t.ID, StatusFailed, nil, "javob kelmadi (timeout/retry tugadi)")
}

// waitOutcome joriy urinish uchun worker javobini, uzilishni yoki timeout'ni kutadi.
func (h *Hub) waitOutcome(t *Task, clientID string) outcome {
	timer := time.NewTimer(h.cfg.TaskTimeout)
	defer timer.Stop()
	for {
		select {
		case ev := <-t.events:
			if ev.clientID != clientID {
				continue // eski/boshqa urinishdan kelgan signal — e'tiborsiz
			}
			switch {
			case ev.disconnect:
				return outcome{kind: outcomeDisconnect, reason: "worker uzildi"}
			case ev.status == StatusDone:
				return outcome{kind: outcomeDone, data: ev.data}
			default:
				reason := ev.errMsg
				if reason == "" {
					reason = "worker xato qaytardi"
				}
				return outcome{kind: outcomeFailed, reason: reason}
			}
		case <-timer.C:
			return outcome{kind: outcomeTimeout, reason: "javob kelmadi (timeout)"}
		}
	}
}

// handleResult client'dan kelgan natijani dispatcher'ga signal qilib uzatadi.
func (h *Hub) handleResult(msg inMessage, clientID string) {
	h.store.emit(msg.TaskID, taskEvent{
		clientID: clientID,
		status:   msg.Status,
		data:     msg.Data,
		errMsg:   msg.Error,
	})
}

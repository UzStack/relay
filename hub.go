package main

import (
	"encoding/json"
	"log"
	"sync"
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
			// shu client'ning ochiq task'larini fail qilamiz
			h.store.FailTasksForClient(c.id)
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

// nextClient round-robin bo'yicha keyingi active client'ni qaytaradi (yo'q bo'lsa nil).
func (h *Hub) nextClient() *Client {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.order) == 0 {
		return nil
	}
	h.rrIndex = h.rrIndex % len(h.order)
	id := h.order[h.rrIndex]
	h.rrIndex++
	return h.clients[id]
}

// ActiveCount active client sonini qaytaradi.
func (h *Hub) ActiveCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

// Dispatch task'ni round-robin tanlangan client'ga yuboradi.
// Active client yo'q bo'lsa task'ni no_worker deb belgilaydi va false qaytaradi.
func (h *Hub) Dispatch(t *Task) bool {
	c := h.nextClient()
	if c == nil {
		h.store.Complete(t.ID, StatusNoWorker, nil, "active client yo'q")
		return false
	}
	msg := outMessage{Type: "task", TaskID: t.ID, Payload: t.Payload}
	data, err := json.Marshal(msg)
	if err != nil {
		h.store.Complete(t.ID, StatusFailed, nil, "marshal xatosi: "+err.Error())
		return false
	}
	// send channel to'lib qolsa yoki yopiq bo'lsa, client sog'lom emas deb hisoblaymiz.
	select {
	case c.send <- data:
		h.store.MarkDispatched(t.ID, c.id)
		return true
	default:
		h.store.Complete(t.ID, StatusNoWorker, nil, "client band (send buffer to'la)")
		return false
	}
}

// handleResult client'dan kelgan natija xabarini qayta ishlaydi.
func (h *Hub) handleResult(msg inMessage) {
	switch msg.Status {
	case StatusDone:
		h.store.Complete(msg.TaskID, StatusDone, msg.Data, "")
	case StatusFailed:
		errMsg := msg.Error
		if errMsg == "" {
			errMsg = "client xatosi"
		}
		h.store.Complete(msg.TaskID, StatusFailed, nil, errMsg)
	default:
		log.Printf("noma'lum status client natijasida: %q (task %s)", msg.Status, msg.TaskID)
	}
}

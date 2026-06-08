package main

import (
	"testing"
)

// addFakeClient hub'ga (WS connection'siz) soxta client qo'shadi — Dispatch faqat
// id va send channel'dan foydalanadi.
func addFakeClient(h *Hub, id string) *Client {
	c := &Client{id: id, hub: h, send: make(chan []byte, sendBuffer)}
	h.clients[id] = c
	h.order = append(h.order, id)
	return c
}

func TestRoundRobin(t *testing.T) {
	h := NewHub(Config{}, NewTaskStore())
	addFakeClient(h, "a")
	addFakeClient(h, "b")
	addFakeClient(h, "c")

	want := []string{"a", "b", "c", "a", "b"}
	for i, w := range want {
		if got := h.nextClient(); got.id != w {
			t.Fatalf("qadam %d: kutilgan %s, olindi %s", i, w, got.id)
		}
	}
}

func TestDispatchNoWorker(t *testing.T) {
	store := NewTaskStore()
	h := NewHub(Config{}, store)
	task := store.Create(nil)

	if h.Dispatch(task) {
		t.Fatal("client yo'qda Dispatch false qaytarishi kerak edi")
	}
	if got, _ := store.Get(task.ID); got.Status != StatusNoWorker {
		t.Fatalf("kutilgan no_worker, olindi %s", got.Status)
	}
}

func TestDispatchSendsToClient(t *testing.T) {
	store := NewTaskStore()
	h := NewHub(Config{}, store)
	c := addFakeClient(h, "a")
	task := store.Create([]byte(`{"k":"v"}`))

	if !h.Dispatch(task) {
		t.Fatal("Dispatch muvaffaqiyatli bo'lishi kerak edi")
	}
	if got, _ := store.Get(task.ID); got.Status != StatusDispatched || got.ClientID != "a" {
		t.Fatalf("dispatched holati noto'g'ri: %+v", got)
	}

	select {
	case msg := <-c.send:
		if len(msg) == 0 {
			t.Fatal("bo'sh xabar yuborildi")
		}
	default:
		t.Fatal("client send channel'iga xabar tushmadi")
	}
}

func TestHandleResultCompletesTask(t *testing.T) {
	store := NewTaskStore()
	h := NewHub(Config{}, store)
	task := store.Create(nil)
	store.MarkDispatched(task.ID, "a")

	h.handleResult(inMessage{Type: "result", TaskID: task.ID, Status: StatusDone, Data: []byte(`42`)})

	got, _ := store.Get(task.ID)
	if got.Status != StatusDone || string(got.Result) != "42" {
		t.Fatalf("done bo'lishi kerak edi: %+v", got)
	}
}

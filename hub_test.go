package main

import (
	"testing"
	"time"
)

// addFakeClient hub'ga (WS connection'siz) soxta client qo'shadi — dispatch faqat
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

	none := map[string]bool{}
	want := []string{"a", "b", "c", "a", "b"}
	for i, w := range want {
		if got := h.nextClientExcluding(none); got.id != w {
			t.Fatalf("qadam %d: kutilgan %s, olindi %s", i, w, got.id)
		}
	}
}

func TestNextClientExcludingSkipsTried(t *testing.T) {
	h := NewHub(Config{}, NewTaskStore())
	addFakeClient(h, "a")
	addFakeClient(h, "b")
	tried := map[string]bool{"a": true}
	if got := h.nextClientExcluding(tried); got == nil || got.id != "b" {
		t.Fatalf("urinilgan 'a' o'tkazib yuborilib 'b' tanlanishi kerak edi, olindi %v", got)
	}
	tried["b"] = true
	if got := h.nextClientExcluding(tried); got != nil {
		t.Fatalf("hamma urinilgan — nil kutilgan, olindi %v", got)
	}
}

func TestDispatchNoWorker(t *testing.T) {
	store := NewTaskStore()
	h := NewHub(Config{TaskTimeout: 50 * time.Millisecond}, store)
	task := store.Create(nil)

	h.Dispatch(task) // client yo'q — darrov no_worker bilan qaytadi
	if got, _ := store.Get(task.ID); got.Status != StatusNoWorker {
		t.Fatalf("kutilgan no_worker, olindi %s", got.Status)
	}
}

func TestDispatchSucceedsOnResult(t *testing.T) {
	store := NewTaskStore()
	h := NewHub(Config{TaskTimeout: time.Second}, store)
	c := addFakeClient(h, "a")
	task := store.Create([]byte(`{"k":"v"}`))

	go func() {
		<-c.send // worker task'ni oldi
		h.handleResult(inMessage{Type: "result", TaskID: task.ID, Status: StatusDone, Data: []byte(`42`)}, "a")
	}()

	h.Dispatch(task)
	got, _ := store.Get(task.ID)
	if got.Status != StatusDone || string(got.Result) != "42" || got.ClientID != "a" {
		t.Fatalf("done bo'lishi kerak edi: %+v", got)
	}
}

func TestDispatchRetriesToAnotherWorker(t *testing.T) {
	store := NewTaskStore()
	h := NewHub(Config{TaskTimeout: 80 * time.Millisecond, MaxRetries: 2}, store)
	a := addFakeClient(h, "a") // javob bermaydi
	b := addFakeClient(h, "b") // javob beradi
	task := store.Create(nil)

	go func() { <-a.send }() // A task'ni oladi-yu, javob bermaydi → timeout
	go func() {
		<-b.send // B ga retry bo'lib keldi
		h.handleResult(inMessage{Type: "result", TaskID: task.ID, Status: StatusDone, Data: []byte(`"ok"`)}, "b")
	}()

	h.Dispatch(task)
	got, _ := store.Get(task.ID)
	if got.Status != StatusDone || got.ClientID != "b" || got.Attempts != 2 {
		t.Fatalf("A timeout → B ga retry → done (attempts=2) kutilgan: %+v", got)
	}
}

func TestDispatchFailsWhenAllWorkersExhausted(t *testing.T) {
	store := NewTaskStore()
	h := NewHub(Config{TaskTimeout: 40 * time.Millisecond, MaxRetries: 2}, store)
	addFakeClient(h, "a") // ikkalasi ham javob bermaydi
	addFakeClient(h, "b")
	task := store.Create(nil)

	h.Dispatch(task) // A timeout, B timeout, boshqa worker yo'q → failed
	got, _ := store.Get(task.ID)
	if got.Status != StatusFailed {
		t.Fatalf("barcha worker urinilgach failed kutilgan, olindi %s", got.Status)
	}
}

func TestDispatchRetriesOnDisconnect(t *testing.T) {
	store := NewTaskStore()
	h := NewHub(Config{TaskTimeout: time.Second, MaxRetries: 2}, store)
	a := addFakeClient(h, "a")
	b := addFakeClient(h, "b")
	task := store.Create(nil)

	go func() {
		<-a.send                      // A task'ni oldi
		store.EmitDisconnect("a")     // ... keyin uzilib qoldi
	}()
	go func() {
		<-b.send
		h.handleResult(inMessage{Type: "result", TaskID: task.ID, Status: StatusDone, Data: []byte(`1`)}, "b")
	}()

	h.Dispatch(task)
	got, _ := store.Get(task.ID)
	if got.Status != StatusDone || got.ClientID != "b" {
		t.Fatalf("A uzilib → B ga retry → done kutilgan: %+v", got)
	}
}

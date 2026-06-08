package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// startTestServer to'liq stack'ni httptest orqali ishga tushiradi.
func startTestServer(t *testing.T) (*httptest.Server, *Hub) {
	t.Helper()
	cfg := Config{
		AuthToken:    "secret",
		PingInterval: 50 * time.Millisecond,
		PongWait:     200 * time.Millisecond,
		WaitTimeout:  2 * time.Second,
	}
	store := NewTaskStore()
	hub := NewHub(cfg, store)
	go hub.Run()
	srv := NewServer(cfg, hub, store)
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)
	return ts, hub
}

// dialWorker test worker'ni ulaydi.
func dialWorker(t *testing.T, ts *httptest.Server) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws?token=secret"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("worker ulanmadi: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestE2E_SyncTask(t *testing.T) {
	ts, hub := startTestServer(t)
	conn := dialWorker(t, ts)

	// worker active bo'lguncha kutamiz
	waitFor(t, func() bool { return hub.ActiveCount() == 1 })

	// worker: task'ni o'qib, echo qilib qaytaradi
	go func() {
		var msg inMessage
		if err := conn.ReadJSON(&msg); err != nil {
			return
		}
		conn.WriteJSON(map[string]any{
			"type":    "result",
			"task_id": msg.TaskID,
			"status":  "done",
			"data":    json.RawMessage(`{"echo":true}`),
		})
	}()

	// sync task yuboramiz
	body := bytes.NewBufferString(`{"payload":{"x":1}}`)
	req, _ := http.NewRequest("POST", ts.URL+"/tasks?wait=true", body)
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("kutilgan 200, olindi %d", resp.StatusCode)
	}
	var task Task
	json.NewDecoder(resp.Body).Decode(&task)
	if task.Status != StatusDone || string(task.Result) != `{"echo":true}` {
		t.Fatalf("noto'g'ri natija: %+v", task)
	}
}

func TestE2E_NoWorker(t *testing.T) {
	ts, _ := startTestServer(t)
	body := bytes.NewBufferString(`{"payload":1}`)
	req, _ := http.NewRequest("POST", ts.URL+"/tasks", body)
	req.Header.Set("Authorization", "Bearer secret")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("kutilgan 503, olindi %d", resp.StatusCode)
	}
}

func TestE2E_Unauthorized(t *testing.T) {
	ts, _ := startTestServer(t)
	resp, _ := http.Get(ts.URL + "/healthz")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("kutilgan 401, olindi %d", resp.StatusCode)
	}
}

func TestE2E_Heartbeat(t *testing.T) {
	ts, hub := startTestServer(t)
	conn := dialWorker(t, ts)
	waitFor(t, func() bool { return hub.ActiveCount() == 1 })

	// worker pong'larga javob bermay to'satdan yopiladi → inactive bo'lishi kerak
	conn.Close()
	waitFor(t, func() bool { return hub.ActiveCount() == 0 })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("shart vaqt ichida bajarilmadi")
}

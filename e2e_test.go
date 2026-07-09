package main

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
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
		WorkerToken:  "secret",
		APIToken:     "secret",
		PingInterval: 50 * time.Millisecond,
		PongWait:     200 * time.Millisecond,
		WaitTimeout:  2 * time.Second,
		TaskTimeout:  500 * time.Millisecond,
		MaxRetries:   2,
		MaxFileSize:  10 << 20,
	}
	store := NewTaskStore()
	hub := NewHub(cfg, store)
	go hub.Run()
	blobs, err := NewBlobStore(t.TempDir(), time.Hour, cfg.MaxFileSize)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(cfg, hub, store, blobs)
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

func TestE2E_TaskTimeoutThenFail(t *testing.T) {
	ts, hub := startTestServer(t)
	conn := dialWorker(t, ts)
	waitFor(t, func() bool { return hub.ActiveCount() == 1 })

	// worker task'ni o'qiydi-yu, javob qaytarmaydi → har urinishda timeout → failed
	go func() {
		var msg inMessage
		for {
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}
		}
	}()

	body := bytes.NewBufferString(`{"payload":{"x":1}}`)
	req, _ := http.NewRequest("POST", ts.URL+"/tasks?wait=true", body)
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("kutilgan 502 (failed), olindi %d", resp.StatusCode)
	}
	var task Task
	json.NewDecoder(resp.Body).Decode(&task)
	if task.Status != StatusFailed {
		t.Fatalf("kutilgan failed, olindi %s", task.Status)
	}
}

func TestE2E_NoWorker(t *testing.T) {
	ts, _ := startTestServer(t)
	body := bytes.NewBufferString(`{"payload":1}`)
	req, _ := http.NewRequest("POST", ts.URL+"/tasks", body)
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("kutilgan 503, olindi %d", resp.StatusCode)
	}
}

func TestE2E_Unauthorized(t *testing.T) {
	ts, _ := startTestServer(t)
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
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

func TestE2E_FileRoundTrip(t *testing.T) {
	ts, _ := startTestServer(t)

	// upload
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	part, _ := mw.CreateFormFile("file", "note.txt")
	part.Write([]byte("salom dunyo"))
	mw.Close()

	req, _ := http.NewRequest("POST", ts.URL+"/files", &buf)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload: kutilgan 201, olindi %d", resp.StatusCode)
	}
	var meta struct {
		FileID string `json:"file_id"`
		Size   int64  `json:"size"`
	}
	json.NewDecoder(resp.Body).Decode(&meta)
	resp.Body.Close()
	if meta.FileID == "" || meta.Size != 11 {
		t.Fatalf("noto'g'ri meta: %+v", meta)
	}

	// download
	dreq, _ := http.NewRequest("GET", ts.URL+"/files/"+meta.FileID, nil)
	dreq.Header.Set("Authorization", "Bearer secret")
	dresp, err := http.DefaultClient.Do(dreq)
	if err != nil {
		t.Fatal(err)
	}
	defer dresp.Body.Close()
	if dresp.StatusCode != http.StatusOK {
		t.Fatalf("download: kutilgan 200, olindi %d", dresp.StatusCode)
	}
	got, _ := io.ReadAll(dresp.Body)
	if string(got) != "salom dunyo" {
		t.Fatalf("noto'g'ri fayl mazmuni: %q", got)
	}
}

func TestE2E_FileNotFound(t *testing.T) {
	ts, _ := startTestServer(t)
	req, _ := http.NewRequest("GET", ts.URL+"/files/yoq-id", nil)
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("kutilgan 404, olindi %d", resp.StatusCode)
	}
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

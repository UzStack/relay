// Command worker — mesh server'iga ulanadigan namuna worker client.
//
// Ishga tushirish:
//
//	MESH_URL=ws://localhost:8080/ws TOKEN=secret go run ./cmd/worker
//
// Worker server'dan task oladi, handle() funksiyasi orqali bajaradi va
// natijani qaytaradi. Ulanish uzilsa avtomatik qayta ulanadi (backoff bilan).
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// taskMessage — server'dan keladigan task.
type taskMessage struct {
	Type    string          `json:"type"`
	TaskID  string          `json:"task_id"`
	Payload json.RawMessage `json:"payload"`
}

// resultMessage — server'ga yuboriladigan natija.
type resultMessage struct {
	Type   string          `json:"type"`
	TaskID string          `json:"task_id"`
	Status string          `json:"status"` // "done" yoki "failed"
	Data   json.RawMessage `json:"data,omitempty"`
	Error  string          `json:"error,omitempty"`
}

const (
	pongWait   = 6 * time.Second // server ping kelmasa shu vaqtdan keyin uzamiz
	writeWait  = 10 * time.Second
	maxBackoff = 30 * time.Second
)

// version build vaqtida ldflags orqali o'rnatiladi (-X main.version=...).
var version = "dev"

func main() {
	log.Printf("mesh-worker versiya: %s", version)
	url := getenv("MESH_URL", "ws://localhost:8080/ws")
	token := os.Getenv("TOKEN")
	if token == "" {
		log.Fatal("TOKEN env o'rnatilishi shart")
	}
	url += "?token=" + token

	backoff := time.Second
	for {
		if err := run(url); err != nil {
			log.Printf("ulanish uzildi: %v — %s dan keyin qayta ulanish", err, backoff)
			time.Sleep(backoff)
			if backoff < maxBackoff {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second // muvaffaqiyatli sessiyadan keyin reset
	}
}

// conn — concurrent-safe yozish uchun WS connection o'rami.
// Barcha yozishlar (natija + pong) bitta mutex orqali ketadi.
type conn struct {
	ws *websocket.Conn
	mu sync.Mutex
}

func (c *conn) writeJSON(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ws.SetWriteDeadline(time.Now().Add(writeWait))
	return c.ws.WriteJSON(v)
}

func (c *conn) writeControl(messageType int, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ws.WriteControl(messageType, data, time.Now().Add(writeWait))
}

// run bitta ulanish sessiyasini boshqaradi; uzilganda error qaytaradi.
func run(url string) error {
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	c := &conn{ws: ws}
	defer ws.Close()
	log.Printf("mesh'ga ulandi: %s", url)

	// Server ping yuboradi; har ping'da read deadline'ni uzaytiramiz va pong qaytaramiz.
	ws.SetReadDeadline(time.Now().Add(pongWait))
	ws.SetPingHandler(func(appData string) error {
		ws.SetReadDeadline(time.Now().Add(pongWait))
		return c.writeControl(websocket.PongMessage, []byte(appData))
	})

	for {
		var msg taskMessage
		if err := ws.ReadJSON(&msg); err != nil {
			return err
		}
		if msg.Type != "task" {
			continue
		}
		// Har task'ni alohida goroutine'da bajaramiz — sekin task o'qishni bloklamasin.
		go func(t taskMessage) {
			if err := c.writeJSON(process(t)); err != nil {
				log.Printf("natija yozish xatosi (task %s): %v", t.TaskID, err)
			}
		}(msg)
	}
}

// process bitta task'ni bajaradi va natija xabarini qaytaradi.
func process(t taskMessage) resultMessage {
	data, err := handle(t.TaskID, t.Payload)
	if err != nil {
		return resultMessage{Type: "result", TaskID: t.TaskID, Status: "failed", Error: err.Error()}
	}
	return resultMessage{Type: "result", TaskID: t.TaskID, Status: "done", Data: data}
}

// httpClient barcha forward so'rovlar uchun (timeout bilan).
var httpClient = &http.Client{Timeout: 30 * time.Second}

// httpTask — payload formati: worker qaysi API'ga, qanday so'rov yuborishini bildiradi.
type httpTask struct {
	Method  string            `json:"method"`  // GET, POST, ... (bo'sh bo'lsa GET)
	URL     string            `json:"url"`     // target API manzili (majburiy)
	Headers map[string]string `json:"headers"` // qo'shimcha header'lar (ixtiyoriy)
	Body    json.RawMessage   `json:"body"`    // yuboriladigan body (ixtiyoriy, JSON)
}

// httpResult — so'rovchiga qaytadigan natija.
type httpResult struct {
	StatusCode int             `json:"status_code"`
	Body       json.RawMessage `json:"body"`
}

// handle — payload'da ko'rsatilgan target API'ga HTTP so'rov yuboradi va javobni qaytaradi.
func handle(taskID string, payload json.RawMessage) (json.RawMessage, error) {
	var task httpTask
	if err := json.Unmarshal(payload, &task); err != nil {
		return nil, fmt.Errorf("payload noto'g'ri: %w", err)
	}
	if task.URL == "" {
		return nil, fmt.Errorf("payload.url majburiy")
	}
	method := task.Method
	if method == "" {
		method = http.MethodGet
	}

	var body io.Reader
	if len(task.Body) > 0 {
		body = bytes.NewReader(task.Body)
	}
	req, err := http.NewRequest(method, task.URL, body)
	if err != nil {
		return nil, fmt.Errorf("so'rov yaratish: %w", err)
	}
	for k, v := range task.Headers {
		req.Header.Set(k, v)
	}
	if len(task.Body) > 0 && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	log.Printf("task %s → %s %s", taskID, method, task.URL)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("target API xatosi: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("javobni o'qish: %w", err)
	}

	out, _ := json.Marshal(httpResult{
		StatusCode: resp.StatusCode,
		Body:       asJSON(raw),
	})
	return out, nil
}

// asJSON javob body'sini JSON bo'lsa o'sha holicha, aks holda string sifatida qaytaradi.
func asJSON(b []byte) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("null")
	}
	if json.Valid(b) {
		return json.RawMessage(b)
	}
	quoted, _ := json.Marshal(string(b))
	return quoted
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

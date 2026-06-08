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
	"encoding/json"
	"log"
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

func main() {
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

// handle — ASOSIY BIZNES MANTIQ shu yerga yoziladi.
// Hozircha namuna sifatida payload'ni echo qiladi.
func handle(taskID string, payload json.RawMessage) (json.RawMessage, error) {
	log.Printf("task qabul qilindi: %s payload=%s", taskID, string(payload))
	// ... bu yerda haqiqiy ishni bajaring (HTTP so'rov, hisoblash va h.k.) ...
	out, _ := json.Marshal(map[string]any{
		"echo":        payloadOrNull(payload),
		"handled_by":  "worker",
		"received_at": time.Now().Format(time.RFC3339),
	})
	return out, nil
}

func payloadOrNull(p json.RawMessage) json.RawMessage {
	if len(p) == 0 {
		return json.RawMessage("null")
	}
	return p
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

# Mesh

WebSocket orqali task tarqatuvchi **fallback** xizmat. Tashqi API'lardan kelgan
so'rovlarni real-time ulangan worker client'larga yuboradi va javobni qaytaradi.

## Qanday ishlaydi

```
   tashqi API ──POST /tasks──▶  mesh  ──WS task──▶  worker client
   tashqi API ◀──javob/task_id── mesh ◀──WS result── worker client
```

- Worker'lar `GET /ws` orqali ulanadi va active deb belgilanadi.
- Task kelsa, **round-robin** bilan active worker tanlanadi va yuboriladi.
- Har task'ga `task_id` beriladi; status va javob `GET /tasks/{id}` orqali olinadi.
- Har **2 soniyada** WS ping yuboriladi; pong kelmasa worker inactive bo'ladi va
  uning ochiq task'lari `failed: worker_disconnected` bo'ladi.

## Ishga tushirish

```bash
AUTH_TOKEN=secret go run .
# :8080 da tinglaydi
```

Sozlamalar (env): `PORT`, `AUTH_TOKEN` (majburiy), `PING_INTERVAL` (2s),
`PONG_WAIT` (6s), `WAIT_TIMEOUT` (30s).

Barcha HTTP so'rovlar `Authorization: Bearer <AUTH_TOKEN>` talab qiladi
(WS uchun `?token=` ham bo'ladi).

## API

| Method | Path | Tavsif |
|--------|------|--------|
| GET | `/ws?token=` | Worker WebSocket ulanishi |
| POST | `/tasks` | Task yuborish (async) |
| POST | `/tasks?wait=true` | Task yuborish (sync — javob kelguncha kutadi) |
| GET | `/tasks/{id}` | Task status va javobini olish |
| GET | `/healthz` | Active client / task statistikasi |

### Async namuna
```bash
curl -H "Authorization: Bearer secret" \
     -d '{"payload":{"x":1}}' localhost:8080/tasks
# → {"task_id":"...","status":"dispatched",...}

curl -H "Authorization: Bearer secret" localhost:8080/tasks/<task_id>
# → {"status":"done","result":{...}}
```

### Sync namuna
```bash
curl -H "Authorization: Bearer secret" \
     -d '{"payload":{"x":1}}' "localhost:8080/tasks?wait=true"
# javob kelguncha bloklab turadi, keyin to'liq natijani qaytaradi
```

## WebSocket protokoli

Server → worker:
```json
{ "type": "task", "task_id": "uuid", "payload": { ... } }
```
Worker → server:
```json
{ "type": "result", "task_id": "uuid", "status": "done", "data": { ... } }
{ "type": "result", "task_id": "uuid", "status": "failed", "error": "..." }
```

## Namuna worker (Go)

```go
package main

import (
	"encoding/json"
	"log"

	"github.com/gorilla/websocket"
)

func main() {
	c, _, err := websocket.DefaultDialer.Dial("ws://localhost:8080/ws?token=secret", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	for {
		var msg struct {
			Type    string          `json:"type"`
			TaskID  string          `json:"task_id"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := c.ReadJSON(&msg); err != nil {
			log.Fatal(err)
		}
		if msg.Type != "task" {
			continue
		}
		// ... task'ni bajarish ...
		c.WriteJSON(map[string]any{
			"type":    "result",
			"task_id": msg.TaskID,
			"status":  "done",
			"data":    map[string]any{"echo": json.RawMessage(msg.Payload)},
		})
	}
}
```

## Test

```bash
go test ./...
```

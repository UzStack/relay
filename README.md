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
  uning ochiq task'lari boshqa worker'ga qayta yuboriladi (pastdagi *Fallback*).

## Ishga tushirish

```bash
AUTH_TOKEN=secret go run .
# :8080 da tinglaydi
```

Sozlamalar (env):

| Var | Default | Tavsif |
|-----|---------|--------|
| `PORT` | `8080` | HTTP/WS port |
| `AUTH_TOKEN` | — (majburiy) | Statik bearer token |
| `PING_INTERVAL` | `2s` | Heartbeat ping oralig'i |
| `PONG_WAIT` | `6s` | Pong kutish (read deadline) |
| `TASK_TIMEOUT` | `10s` | Bitta urinish: worker javobini kutish, so'ng retry |
| `MAX_RETRIES` | `2` | Javob kelmasa nechta BOSHQA worker'ga qayta yuborish |
| `WAIT_TIMEOUT` | `35s` | `?wait=true` uchun maksimal HTTP kutish |

## Fallback qanday ishlaydi

Har task uchun server ichida alohida **dispatcher** ishlaydi:

```
task → worker-A ga yuborildi
       ├─ TASK_TIMEOUT ichida javob keldi? → done ✅
       ├─ worker xato qaytardi?            → boshqa worker'ga retry
       ├─ worker uzilib qoldi?             → boshqa worker'ga retry
       └─ javob umuman kelmadi (timeout)?  → boshqa worker'ga retry
              ... MAX_RETRIES martagacha, har safar BOSHQA (urinilmagan) worker'ga ...
                     └─ hammasi uddasidan chiqmasa → failed
```

- Birinchi kelgan javob **g'olib** (kechikkan/takroriy javoblar e'tiborsiz).
- Hech qaysi worker ulanmagan bo'lsa → darrov `no_worker` (503).
- `attempts` maydoni nechta worker'ga urinilganini ko'rsatadi.

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

## Worker client

Tayyor worker client `cmd/worker/` ichida. Ulanish uzilsa avtomatik qayta
ulanadi (exponential backoff), task'larni parallel bajaradi va heartbeat'ga
javob beradi.

```bash
MESH_URL=ws://localhost:8080/ws TOKEN=secret go run ./cmd/worker
```

Worker `handle()` (`cmd/worker/main.go`) payload'ni **target API'ga so'rov
ko'rsatmasi** deb o'qiydi va o'sha API'ga HTTP so'rov yuborib, javobni qaytaradi.

Payload formati:

```json
{
  "method":  "POST",                       // GET, POST... (bo'sh = GET)
  "url":     "https://example.com/api/",   // target API (majburiy)
  "headers": { "X-Api-Key": "..." },       // ixtiyoriy
  "body":    { "summa": 1000 }             // yuboriladigan data (ixtiyoriy)
}
```

Worker qaytaradigan natija: `{ "status_code": 200, "body": {...} }` — target API
javobi. Boshqacha mantiq kerak bo'lsa (HTTP emas, hisoblash va h.k.) — `handle()`
funksiyasini o'zgartiring.

Worker env: `MESH_URL` (default `ws://localhost:8080/ws`), `TOKEN` (majburiy).

### Misol — boshqa API'ga data yuborish

```bash
curl -H "Authorization: Bearer secret" \
  -d '{
    "payload": {
      "method": "POST",
      "url": "https://example.com/api/",
      "body": { "summa": 1000, "user_id": 42 }
    }
  }' \
  "localhost:8080/tasks?wait=true"
```

## Test

```bash
go test ./...
```

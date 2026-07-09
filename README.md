# Relay

WebSocket orqali task tarqatuvchi **fallback** xizmat. Tashqi API'lardan kelgan
so'rovlarni real-time ulangan worker client'larga yuboradi va javobni qaytaradi.

## Qanday ishlaydi

```
   tashqi API ──POST /tasks──▶  relay  ──WS task──▶  worker client
   tashqi API ◀──javob/task_id── relay ◀──WS result── worker client
```

- Worker'lar `GET /ws` orqali ulanadi va active deb belgilanadi.
- Task kelsa, **round-robin** bilan active worker tanlanadi va yuboriladi.
- Har task'ga `task_id` beriladi; status va javob `GET /tasks/{id}` orqali olinadi.
- Har **2 soniyada** WS ping yuboriladi; pong kelmasa worker inactive bo'ladi va
  uning ochiq task'lari boshqa worker'ga qayta yuboriladi (pastdagi *Fallback*).

## Ishga tushirish

```bash
WORKER_TOKEN=wtok API_TOKEN=atok go run .
# :8080 da tinglaydi
```

Sozlamalar (env):

| Var | Default | Tavsif |
|-----|---------|--------|
| `PORT` | `8080` | HTTP/WS port |
| `WORKER_TOKEN` | — (majburiy) | Worker'lar `/ws` ga ulanish tokeni |
| `API_TOKEN` | — (majburiy) | Task yuborish/o'qish API tokeni |
| `AUTH_TOKEN` | — | Eski yagona token: yuqoridagilardan biri berilmasa shu ishlatiladi |
| `PING_INTERVAL` | `2s` | Heartbeat ping oralig'i |
| `PONG_WAIT` | `6s` | Pong kutish (read deadline) |
| `TASK_TIMEOUT` | `10s` | Bitta urinish: worker javobini kutish, so'ng retry |
| `MAX_RETRIES` | `2` | Javob kelmasa nechta BOSHQA worker'ga qayta yuborish |
| `WAIT_TIMEOUT` | `35s` | `?wait=true` uchun maksimal HTTP kutish |
| `BLOB_DIR` | `$TMPDIR/relay-blobs` | Yuklangan fayllar saqlanadigan katalog |
| `BLOB_TTL` | `1h` | Fayl qancha saqlanadi (so'ng avtomatik o'chiriladi) |
| `MAX_FILE_SIZE` | `104857600` (100 MiB) | Bitta fayl uchun maksimal hajm (bayt) |
| `TASK_TTL` | `1h` | Yakunlangan task qancha saqlanadi (so'ng xotiradan o'chiriladi) |
| `MAX_MESSAGE_SIZE` | `1048576` (1 MiB) | Worker'dan keladigan WS xabar uchun maksimal hajm (bayt) |
| `TOKEN_SECRET` | — | Scoped JWT token'larni imzolash siri (bo'sh = o'chirilgan) |
| `TOKEN_STORE` | `relay-tokens.json` | Token reyestri (JSON) fayl yo'li |

> **Holat in-memory:** task'lar, ularning natijalari va yuklangan fayllar faqat
> xotirada/diskda vaqtincha saqlanadi — server qayta ishga tushsa hammasi
> yo'qoladi (persistence yo'q). Yakunlangan task'lar `TASK_TTL`, fayllar
> `BLOB_TTL` o'tgach avtomatik o'chiriladi (xotira cheksiz o'smaydi).

**Ikkita alohida token:**
- **`WORKER_TOKEN`** — worker'lar WebSocket bilan ulanish uchun (`TOKEN` env'i shu bo'ladi).
- **`API_TOKEN`** — tashqi xizmatlar task yuborish/o'qish uchun (`Authorization: Bearer`).

Shu ikki token bir-biridan farq qiladi: worker tokeni bilan API'ga so'rov yuborib
bo'lmaydi va aksincha. Faqat bittasini berib (yoki eski `AUTH_TOKEN` bilan)
ikkalasi uchun bir xil token ham ishlatsa bo'ladi.

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

### Javobdagi tarix (kim, qachon, nega fail bo'ldi)

Har task javobida to'liq diagnostika qaytadi:

| Maydon | Ma'nosi |
|--------|---------|
| `client_id` | taskni **bajargan** worker UUID'si |
| `client_ip` | shu worker IP'si |
| `attempts` | jami necha marta urinilgani |
| `attempt_log[]` | har urinish: `{num, client_id, client_ip, outcome, error}` |

`outcome` qiymatlari: `done`, `timeout`, `failed`, `disconnect`, `send_failed`.

```json
"attempt_log": [
  { "num": 1, "client_id": "d9c3...", "client_ip": "10.0.0.5", "outcome": "timeout", "error": "javob kelmadi (timeout)" },
  { "num": 2, "client_id": "1766...", "client_ip": "10.0.0.6", "outcome": "done" }
]
```

Har worker ulanganda **UUID** oladi va uning **IP'si** saqlanadi — `GET /clients`
orqali hammasini ko'rish mumkin:

```bash
curl -H "Authorization: Bearer secret" localhost:8080/clients
# → { "count": 2, "clients": [ {"client_id":"...","ip":"10.0.0.5","connected_at":"..."}, ... ] }
```

Barcha task API so'rovlari `Authorization: Bearer <API_TOKEN>` talab qiladi.
Worker WS ulanishi esa `?token=<WORKER_TOKEN>` orqali autentifikatsiya qilinadi.

## Scoped token'lar (JWT, kind bo'yicha cheklangan)

`API_TOKEN` — barcha kind'larga ruxsat beruvchi **root** token. Undan tashqari,
faqat ma'lum **kind'larga** ruxsat beruvchi, **bekor qilinadigan** JWT token'lar
chiqarish mumkin. Yoqish uchun `TOKEN_SECRET` o'rnatiladi.

Token'lar `relay token` CLI orqali boshqariladi (server bilan bir xil
`TOKEN_SECRET` va `TOKEN_STORE` kerak):

```bash
export TOKEN_SECRET=uzun-tasodifiy-sir
export TOKEN_STORE=/var/lib/relay/tokens.json   # server bilan bir xil

# faqat "http" kind'ga ruxsat beruvchi, 30 kun amal qiladigan token
relay token create --name billing-service --kinds http --ttl 720h
# → jti, name, kinds, expires va token (JWT) chop etiladi
#   (--name ixtiyoriy yorliq: kim/nima uchun ekanini bildiradi)

relay token list             # barcha token'lar (active/revoked/expired)
relay token revoke <jti>     # token'ni bekor qilish (darhol kuchga kiradi)
```

So'rovda shu token ishlatiladi:

```bash
curl -H "Authorization: Bearer <JWT>" \
  -d '{"payload":{"kind":"http","spec":{...}}}' localhost:8080/tasks
```

- `payload.kind` token ruxsatidagi kind'lardan bo'lishi shart — aks holda **403**.
- Token bekor qilingan / muddati o'tgan / imzo noto'g'ri bo'lsa — **401**.
- Root `API_TOKEN` esa kind tekshiruvidan o'tmaydi (hammasiga ruxsat).

**Ishlash tartibi:** token amal qilishi uchun (1) imzo `TOKEN_SECRET` bilan
to'g'ri, (2) muddati o'tmagan, (3) `jti` reyestrda mavjud va bekor qilinmagan
bo'lishi kerak (*fail-closed* — reyestr yo'qolsa token ishlamaydi, shu bois
revoke ishonchli). Reyestr diskda saqlanadi va restart'dan omon qoladi.

## API

| Method | Path | Tavsif |
|--------|------|--------|
| GET | `/ws?token=` | Worker WebSocket ulanishi |
| POST | `/tasks` | Task yuborish (async) |
| POST | `/tasks?wait=true` | Task yuborish (sync — javob kelguncha kutadi) |
| GET | `/tasks/{id}` | Task status, javob va urinishlar tarixini olish |
| GET | `/clients` | Ulangan worker'lar ro'yxati (id, ip, vaqt) |
| GET | `/healthz` | Active client / task statistikasi |
| POST | `/files` | Fayl yuklash (multipart `file` maydoni) → `file_id` |
| GET | `/files/{id}` | Fayl yuklab olish (`file_id` bo'yicha) |

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

## Fayl uzatish (upload / download)

Fayllar task pipeline'i (JSON/WebSocket) orqali emas, **alohida HTTP kanali**
orqali yuriladi: fayl baytlari diskka yoziladi, task payload'iga esa faqat
`file_id` (reference) uzatiladi. Shu tufayli WebSocket yengil qoladi va katta
fayllar RAM'ni to'ldirmaydi. Fayllar `BLOB_TTL` o'tgach avtomatik o'chiriladi.

`/files` endpoint'lari **ham API, ham WORKER** tokenini qabul qiladi (ikkala tomon
ishlatadi).

**Upload (tashqi client → worker):**

```bash
# 1) faylni yuklaymiz → file_id olamiz
curl -H "Authorization: Bearer atok" -F "file=@hujjat.pdf" localhost:8080/files
# → {"file_id":"...","filename":"hujjat.pdf","content_type":"application/pdf","size":12345,...}

# 2) file_id'ni task spec'ida uzatamiz
curl -H "Authorization: Bearer atok" \
  -d '{"payload":{"kind":"file","spec":{"file_id":"<file_id>"}}}' \
  "localhost:8080/tasks?wait=true"
# → worker faylni GET /files/<id> orqali yuklab oladi, qayta ishlaydi
```

**Download (worker → tashqi client):**
Worker natija faylni `POST /files` orqali yuklaydi va javobda `result_file_id`
qaytaradi; client uni `GET /files/{id}` orqali oladi:

```bash
curl -H "Authorization: Bearer atok" -OJ localhost:8080/files/<result_file_id>
```

Worker tomonida `cmd/worker/files.go` shu ikki amalni (`Download`/`Upload`)
bajaradi; namuna `file` handler'i `cmd/worker/handlers.go` da.

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
RELAY_URL=ws://localhost:8080/ws TOKEN=secret go run ./cmd/worker
```

Worker env: `RELAY_URL` (default `ws://localhost:8080/ws`; eski `MESH_URL` ham
ishlaydi), `TOKEN` (majburiy — server'ning **`WORKER_TOKEN`**'iga teng bo'lishi
kerak).

## Task turlari (kind)

Server payload'ni **o'zgartirmaydi** — uni shundayligicha worker'ga uzatadi.
Worker payload ichidagi **`kind`** (task turi) ga qarab tegishli ishni bajaradi.
Payload formati:

```json
{
  "kind": "http",          // task turi (majburiy)
  "spec": { ... }          // shu turga xos parametrlar
}
```

Hozir ikkita tur mavjud:
- **`http`** — `spec`'da ko'rsatilgan API'ga so'rov yuborib, javobini qaytaradi.
- **`file`** — `spec.file_id` orqali faylni relay'dan yuklab oladi, qayta ishlaydi
  va natijani yangi fayl sifatida yuklab `result_file_id` qaytaradi (yuqoridagi
  *Fayl uzatish* bo'limiga qarang).

`http` spec:

```json
{
  "kind": "http",
  "spec": {
    "method":  "POST",                       // GET, POST... (bo'sh = GET)
    "url":     "https://example.com/api/",   // target API (majburiy)
    "headers": { "X-Api-Key": "..." },       // ixtiyoriy
    "body":    { "summa": 1000 }             // yuboriladigan data (ixtiyoriy)
  }
}
```

`http` natijasi: `{ "status_code": 200, "body": {...} }` — target API javobi.

### Yangi task turi qo'shish

Faqat **worker** o'zgaradi (server umumiy bo'lib qoladi). `cmd/worker/handlers.go`:

1. `Handler` imzosiga mos funksiya yozing:
   `func(taskID string, spec json.RawMessage) (json.RawMessage, error)`
2. uni `handlers` map'iga kalit (kind) bilan qo'shing, masalan:

```go
var handlers = map[string]Handler{
    "http":  httpHandler,
    "email": emailHandler,   // yangi tur
}
```

So'ngra `{"kind":"email","spec":{...}}` payload bilan task yuborsangiz, worker
shu handler'ni ishga tushiradi. Noma'lum `kind` bo'lsa task `failed` bo'ladi va
`attempt_log` da `noma'lum task turi` xatosi ko'rinadi.

### Misol — `http` task yuborish

```bash
curl -H "Authorization: Bearer secret" \
  -d '{
    "payload": {
      "kind": "http",
      "spec": {
        "method": "POST",
        "url": "https://example.com/api/",
        "body": { "summa": 1000, "user_id": 42 }
      }
    }
  }' \
  "localhost:8080/tasks?wait=true"
```

## Test

```bash
go test ./...
```

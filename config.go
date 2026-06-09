package main

import (
	"log"
	"os"
	"strconv"
	"time"
)

// Config holadi xizmatning barcha sozlamalarini (env'dan o'qiladi).
type Config struct {
	Port         string        // HTTP/WS port
	WorkerToken  string        // worker'lar /ws ga ulanishi uchun token
	APIToken     string        // task yuborish/o'qish API'si uchun token
	PingInterval time.Duration // heartbeat ping oralig'i
	PongWait     time.Duration // pong kutish (read deadline)
	WaitTimeout  time.Duration // ?wait=true uchun maksimal HTTP kutish
	TaskTimeout  time.Duration // bitta urinish uchun: worker javobini kutish (so'ng retry)
	MaxRetries   int           // javob kelmasa nechta BOSHQA worker'ga qayta yuborish
}

// LoadConfig env'dan sozlamalarni o'qiydi.
//
// Token'lar ikkita, alohida:
//   - WORKER_TOKEN — worker WebSocket ulanishi (/ws) uchun
//   - API_TOKEN    — task yuborish/o'qish HTTP API'si uchun
//
// Moslik uchun: agar bittasi berilmasa, eski AUTH_TOKEN qiymati ishlatiladi.
func LoadConfig() Config {
	authToken := os.Getenv("AUTH_TOKEN") // eski yagona token (fallback)
	cfg := Config{
		Port:         getenv("PORT", "8080"),
		WorkerToken:  getenv("WORKER_TOKEN", authToken),
		APIToken:     getenv("API_TOKEN", authToken),
		PingInterval: getdur("PING_INTERVAL", 2*time.Second),
		PongWait:     getdur("PONG_WAIT", 6*time.Second),
		WaitTimeout:  getdur("WAIT_TIMEOUT", 35*time.Second),
		TaskTimeout:  getdur("TASK_TIMEOUT", 10*time.Second),
		MaxRetries:   getint("MAX_RETRIES", 2),
	}
	if cfg.WorkerToken == "" {
		log.Fatal("WORKER_TOKEN (yoki AUTH_TOKEN) env o'rnatilishi shart")
	}
	if cfg.APIToken == "" {
		log.Fatal("API_TOKEN (yoki AUTH_TOKEN) env o'rnatilishi shart")
	}
	return cfg
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getint(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("ogohlantirish: %s noto'g'ri son, default ishlatiladi: %d", key, def)
	}
	return def
}

func getdur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Printf("ogohlantirish: %s noto'g'ri duration, default ishlatiladi: %s", key, def)
	}
	return def
}

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
	AuthToken    string        // statik bearer token (client + API uchun)
	PingInterval time.Duration // heartbeat ping oralig'i
	PongWait     time.Duration // pong kutish (read deadline)
	WaitTimeout  time.Duration // ?wait=true uchun maksimal HTTP kutish
	TaskTimeout  time.Duration // bitta urinish uchun: worker javobini kutish (so'ng retry)
	MaxRetries   int           // javob kelmasa nechta BOSHQA worker'ga qayta yuborish
}

// LoadConfig env'dan sozlamalarni o'qiydi. AUTH_TOKEN majburiy.
func LoadConfig() Config {
	cfg := Config{
		Port:         getenv("PORT", "8080"),
		AuthToken:    os.Getenv("AUTH_TOKEN"),
		PingInterval: getdur("PING_INTERVAL", 2*time.Second),
		PongWait:     getdur("PONG_WAIT", 6*time.Second),
		WaitTimeout:  getdur("WAIT_TIMEOUT", 35*time.Second),
		TaskTimeout:  getdur("TASK_TIMEOUT", 10*time.Second),
		MaxRetries:   getint("MAX_RETRIES", 2),
	}
	if cfg.AuthToken == "" {
		log.Fatal("AUTH_TOKEN env o'rnatilishi shart")
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

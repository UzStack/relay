package main

import (
	"log"
	"os"
	"time"
)

// Config holadi xizmatning barcha sozlamalarini (env'dan o'qiladi).
type Config struct {
	Port         string        // HTTP/WS port
	AuthToken    string        // statik bearer token (client + API uchun)
	PingInterval time.Duration // heartbeat ping oralig'i
	PongWait     time.Duration // pong kutish (read deadline)
	WaitTimeout  time.Duration // ?wait=true uchun maksimal kutish
}

// LoadConfig env'dan sozlamalarni o'qiydi. AUTH_TOKEN majburiy.
func LoadConfig() Config {
	cfg := Config{
		Port:         getenv("PORT", "8080"),
		AuthToken:    os.Getenv("AUTH_TOKEN"),
		PingInterval: getdur("PING_INTERVAL", 2*time.Second),
		PongWait:     getdur("PONG_WAIT", 6*time.Second),
		WaitTimeout:  getdur("WAIT_TIMEOUT", 30*time.Second),
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

func getdur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Printf("ogohlantirish: %s noto'g'ri duration, default ishlatiladi: %s", key, def)
	}
	return def
}

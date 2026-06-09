package main

import (
	"log"
	"net/http"
)

// version build vaqtida ldflags orqali o'rnatiladi (-X main.version=...).
var version = "dev"

func main() {
	cfg := LoadConfig()
	log.Printf("mesh versiya: %s", version)

	store := NewTaskStore()
	hub := NewHub(cfg, store)
	go hub.Run()

	srv := NewServer(cfg, hub, store)

	addr := ":" + cfg.Port
	log.Printf("mesh ishga tushdi: %s (ping interval: %s)", addr, cfg.PingInterval)
	if err := http.ListenAndServe(addr, srv.Routes()); err != nil {
		log.Fatalf("server xatosi: %v", err)
	}
}

package main

import (
	"log"
	"net/http"
)

func main() {
	cfg := LoadConfig()

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

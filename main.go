package main

import (
	"log"
	"net/http"
)

// version build vaqtida ldflags orqali o'rnatiladi (-X main.version=...).
var version = "dev"

func main() {
	cfg := LoadConfig()
	log.Printf("relay versiya: %s", version)

	store := NewTaskStore()
	hub := NewHub(cfg, store)
	go hub.Run()

	blobs, err := NewBlobStore(cfg.BlobDir, cfg.BlobTTL, cfg.MaxFileSize)
	if err != nil {
		log.Fatalf("blob store: %v", err)
	}
	go blobs.RunGC(cfg.BlobTTL / 2)

	srv := NewServer(cfg, hub, store, blobs)

	addr := ":" + cfg.Port
	log.Printf("relay ishga tushdi: %s (ping interval: %s)", addr, cfg.PingInterval)
	if err := http.ListenAndServe(addr, srv.Routes()); err != nil {
		log.Fatalf("server xatosi: %v", err)
	}
}

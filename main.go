package main

import (
	"log"
	"net/http"
	"os"
)

// version build vaqtida ldflags orqali o'rnatiladi (-X main.version=...).
var version = "dev"

func main() {
	// `relay token ...` — scoped token'larni boshqarish CLI'si (server'ni ishga tushirmaydi).
	if len(os.Args) > 1 && os.Args[1] == "token" {
		tokenCLI(os.Args[2:])
		return
	}

	cfg := LoadConfig()
	log.Printf("relay versiya: %s", version)

	store := NewTaskStore()
	go store.RunGC(cfg.TaskTTL/2, cfg.TaskTTL)
	hub := NewHub(cfg, store)
	go hub.Run()

	blobs, err := NewBlobStore(cfg.BlobDir, cfg.BlobTTL, cfg.MaxFileSize)
	if err != nil {
		log.Fatalf("blob store: %v", err)
	}
	go blobs.RunGC(cfg.BlobTTL / 2)

	// TOKEN_SECRET berilgan bo'lsa — scoped JWT token'lar yoqiladi.
	var tokens *TokenRegistry
	if cfg.TokenSecret != "" {
		tokens = NewTokenRegistry(cfg.TokenStore, []byte(cfg.TokenSecret))
		log.Printf("scoped JWT token'lar yoqilgan (reyestr: %s)", cfg.TokenStore)
	}

	srv := NewServer(cfg, hub, store, blobs, tokens)

	addr := ":" + cfg.Port
	log.Printf("relay ishga tushdi: %s (ping interval: %s)", addr, cfg.PingInterval)
	if err := http.ListenAndServe(addr, srv.Routes()); err != nil {
		log.Fatalf("server xatosi: %v", err)
	}
}

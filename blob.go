package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// BlobMeta — saqlangan fayl haqidagi metama'lumot (API javobi uchun).
type BlobMeta struct {
	ID          string    `json:"file_id"`
	Filename    string    `json:"filename"`
	ContentType string    `json:"content_type"`
	Size        int64     `json:"size"`
	CreatedAt   time.Time `json:"created_at"`
}

var (
	// ErrBlobNotFound — so'ralgan fayl yo'q (yoki TTL o'tib o'chirilgan).
	ErrBlobNotFound = errors.New("fayl topilmadi")
	// ErrBlobTooLarge — fayl hajmi ruxsat etilgandan katta.
	ErrBlobTooLarge = errors.New("fayl hajmi limitdan oshib ketdi")
)

// BlobStore — diskka yoziladigan fayl ombori. Baytlar diskda, metama'lumot RAM'da.
// Fayllar TTL o'tgach GC orqali avtomatik o'chiriladi. Goroutine-safe.
//
// Metama'lumot RAM'da bo'lgani uchun restart barcha fayllarni "unutadi" —
// shu sabab NewBlobStore ishga tushganda katalogni tozalaydi (task store ham
// in-memory bo'lgani bilan izchil: qayta ishga tushirish holatni saqlamaydi).
type BlobStore struct {
	dir     string
	ttl     time.Duration
	maxSize int64

	mu    sync.RWMutex
	metas map[string]BlobMeta
}

// NewBlobStore omborni tayyorlaydi: katalogni yaratadi va begona fayllarni tozalaydi.
func NewBlobStore(dir string, ttl time.Duration, maxSize int64) (*BlobStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("blob katalogini yaratish: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("blob katalogini o'qish: %w", err)
	}
	for _, e := range entries {
		os.RemoveAll(filepath.Join(dir, e.Name()))
	}
	return &BlobStore{
		dir:     dir,
		ttl:     ttl,
		maxSize: maxSize,
		metas:   make(map[string]BlobMeta),
	}, nil
}

// Put r'dan baytlarni yangi faylga stream qiladi va metama'lumot qaytaradi.
// maxSize'dan oshsa yarim faylni o'chirib ErrBlobTooLarge qaytaradi.
func (s *BlobStore) Put(filename, contentType string, r io.Reader) (BlobMeta, error) {
	id := uuid.NewString()
	path := filepath.Join(s.dir, id)
	f, err := os.Create(path)
	if err != nil {
		return BlobMeta{}, fmt.Errorf("fayl yaratish: %w", err)
	}
	// maxSize+1 gacha o'qiymiz: n > maxSize bo'lsa limit oshgani.
	n, copyErr := io.Copy(f, io.LimitReader(r, s.maxSize+1))
	closeErr := f.Close()
	if copyErr != nil {
		os.Remove(path)
		return BlobMeta{}, fmt.Errorf("fayl yozish: %w", copyErr)
	}
	if closeErr != nil {
		os.Remove(path)
		return BlobMeta{}, fmt.Errorf("faylni yopish: %w", closeErr)
	}
	if n > s.maxSize {
		os.Remove(path)
		return BlobMeta{}, ErrBlobTooLarge
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	meta := BlobMeta{
		ID:          id,
		Filename:    sanitizeFilename(filename),
		ContentType: contentType,
		Size:        n,
		CreatedAt:   time.Now(),
	}
	s.mu.Lock()
	s.metas[id] = meta
	s.mu.Unlock()
	return meta, nil
}

// Open faylni o'qish uchun ochadi va metama'lumotini qaytaradi.
// Chaqiruvchi *os.File'ni yopishi shart. Fayl bo'lmasa ErrBlobNotFound.
//
// id avval RAM'dagi metama'lumot bilan tekshiriladi — begona id disk yo'liga
// aylanmasidan oldin rad etiladi (path-traversal himoyasi).
func (s *BlobStore) Open(id string) (BlobMeta, *os.File, error) {
	s.mu.RLock()
	meta, ok := s.metas[id]
	s.mu.RUnlock()
	if !ok {
		return BlobMeta{}, nil, ErrBlobNotFound
	}
	f, err := os.Open(filepath.Join(s.dir, id))
	if err != nil {
		return BlobMeta{}, nil, ErrBlobNotFound
	}
	return meta, f, nil
}

// GC muddati (TTL) o'tgan fayllarni o'chiradi.
func (s *BlobStore) GC() {
	cutoff := time.Now().Add(-s.ttl)
	s.mu.Lock()
	var expired []string
	for id, m := range s.metas {
		if m.CreatedAt.Before(cutoff) {
			expired = append(expired, id)
			delete(s.metas, id)
		}
	}
	s.mu.Unlock()
	for _, id := range expired {
		os.Remove(filepath.Join(s.dir, id))
	}
}

// RunGC har interval'da GC ishga tushiradi (goroutine'da chaqiriladi).
func (s *BlobStore) RunGC(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		s.GC()
	}
}

// sanitizeFilename fayl nomidan katalog va boshqaruv belgilarini olib tashlaydi
// (Content-Disposition header injection'ining oldini oladi).
func sanitizeFilename(name string) string {
	name = filepath.Base(name)
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || r == '"' || r == '\\' {
			return -1
		}
		return r
	}, name)
	if name == "." || name == string(filepath.Separator) {
		return ""
	}
	return name
}

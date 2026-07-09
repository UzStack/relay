package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// Handler — bitta task turini bajaruvchi funksiya.
// spec — shu turga xos parametrlar (taskEnvelope.Spec). Natija JSON qaytariladi.
type Handler func(taskID string, spec json.RawMessage) (json.RawMessage, error)

// handlers — task turi (kind) -> handler.
//
// Yangi task turi qo'shish uchun:
//  1. shu fayl(yoki yangi fayl)da Handler imzosiga mos funksiya yozing;
//  2. uni quyidagi map'ga kalit (kind) bilan qo'shing.
//
// Server hech narsani o'zgartirmaydi — u faqat payload'ni worker'ga uzatadi.
var handlers = map[string]Handler{
	"http": httpHandler,
	"file": fileHandler,
	// "shell": shellHandler,   // masalan kelajakda
	// "email": emailHandler,
}

// --- http task turi --------------------------------------------------------

// httpClient barcha forward so'rovlar uchun (timeout bilan).
var httpClient = &http.Client{Timeout: 30 * time.Second}

// httpSpec — "http" task spec'i: worker qaysi API'ga, qanday so'rov yuborishini bildiradi.
type httpSpec struct {
	Method  string            `json:"method"`  // GET, POST, ... (bo'sh bo'lsa GET)
	URL     string            `json:"url"`     // target API manzili (majburiy)
	Headers map[string]string `json:"headers"` // qo'shimcha header'lar (ixtiyoriy)
	Body    json.RawMessage   `json:"body"`    // yuboriladigan body (ixtiyoriy, JSON)
}

// httpResult — so'rovchiga qaytadigan natija.
type httpResult struct {
	StatusCode int             `json:"status_code"`
	Body       json.RawMessage `json:"body"`
}

// httpHandler spec'da ko'rsatilgan target API'ga HTTP so'rov yuboradi va javobni qaytaradi.
func httpHandler(taskID string, spec json.RawMessage) (json.RawMessage, error) {
	var s httpSpec
	if err := json.Unmarshal(spec, &s); err != nil {
		return nil, fmt.Errorf("http spec noto'g'ri: %w", err)
	}
	if s.URL == "" {
		return nil, fmt.Errorf("http.spec.url majburiy")
	}
	method := s.Method
	if method == "" {
		method = http.MethodGet
	}

	var body io.Reader
	if len(s.Body) > 0 {
		body = bytes.NewReader(s.Body)
	}
	req, err := http.NewRequest(method, s.URL, body)
	if err != nil {
		return nil, fmt.Errorf("so'rov yaratish: %w", err)
	}
	for k, v := range s.Headers {
		req.Header.Set(k, v)
	}
	if len(s.Body) > 0 && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	log.Printf("task %s [http] → %s %s", taskID, method, s.URL)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("target API xatosi: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("javobni o'qish: %w", err)
	}

	out, _ := json.Marshal(httpResult{
		StatusCode: resp.StatusCode,
		Body:       asJSON(raw),
	})
	return out, nil
}

// --- file task turi --------------------------------------------------------

// fileSpec — "file" task turi spec'i.
type fileSpec struct {
	FileID string `json:"file_id"` // qayta ishlanadigan kirish fayli (majburiy)
}

// fileResult — natija: qayta ishlangan fayl ombordagi yangi id bilan.
type fileResult struct {
	InputSize    int    `json:"input_size"`
	ResultFileID string `json:"result_file_id"`
}

// fileHandler kirish faylni relay'dan yuklab oladi, oddiy "qayta ishlash" qilib
// natijani yangi fayl sifatida yuklaydi va uning file_id'sini qaytaradi.
// Bu download (worker kirishni oladi) va upload (worker natijani yuklaydi)
// ikkala yo'nalishni ham namoyish qiladi.
func fileHandler(taskID string, spec json.RawMessage) (json.RawMessage, error) {
	if files == nil {
		return nil, fmt.Errorf("fayl klienti sozlanmagan")
	}
	var s fileSpec
	if err := json.Unmarshal(spec, &s); err != nil {
		return nil, fmt.Errorf("file spec noto'g'ri: %w", err)
	}
	if s.FileID == "" {
		return nil, fmt.Errorf("file.spec.file_id majburiy")
	}

	data, err := files.Download(s.FileID)
	if err != nil {
		return nil, fmt.Errorf("kirish faylni yuklash: %w", err)
	}
	log.Printf("task %s [file] → kirish %d bayt yuklab olindi", taskID, len(data))

	// Namuna "qayta ishlash": shu baytlarni natija fayl sifatida qaytaramiz.
	resultID, err := files.Upload("processed-"+s.FileID, "application/octet-stream", data)
	if err != nil {
		return nil, fmt.Errorf("natija faylni yuklash: %w", err)
	}

	out, _ := json.Marshal(fileResult{InputSize: len(data), ResultFileID: resultID})
	return out, nil
}

// asJSON javob body'sini JSON bo'lsa o'sha holicha, aks holda string sifatida qaytaradi.
func asJSON(b []byte) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("null")
	}
	if json.Valid(b) {
		return json.RawMessage(b)
	}
	quoted, _ := json.Marshal(string(b))
	return quoted
}

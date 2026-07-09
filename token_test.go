package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func newTestRegistry(t *testing.T) *TokenRegistry {
	t.Helper()
	return NewTokenRegistry(filepath.Join(t.TempDir(), "tokens.json"), []byte("testsecret"))
}

func TestToken_IssueParseRoundTrip(t *testing.T) {
	reg := newTestRegistry(t)
	tok, rec, err := reg.Issue("", []string{"http", "email"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := reg.Parse(tok)
	if err != nil {
		t.Fatalf("Parse xatosi: %v", err)
	}
	if claims.ID != rec.JTI || len(claims.Kinds) != 2 || claims.Kinds[0] != "http" {
		t.Fatalf("noto'g'ri claim'lar: %+v", claims)
	}
}

func TestToken_NamePersisted(t *testing.T) {
	reg := newTestRegistry(t)
	_, rec, err := reg.Issue("billing-service", []string{"http"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Name != "billing-service" {
		t.Fatalf("Issue name'ni qaytarmadi: %+v", rec)
	}
	// reyestrdan (fayldan) qayta o'qilganda ham saqlanishi kerak
	recs, _ := reg.List()
	if len(recs) != 1 || recs[0].Name != "billing-service" {
		t.Fatalf("name reyestrda saqlanmadi: %+v", recs)
	}
}

func TestToken_RevokedRejected(t *testing.T) {
	reg := newTestRegistry(t)
	tok, rec, _ := reg.Issue("", []string{"http"}, time.Hour)
	if err := reg.Revoke(rec.JTI); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Parse(tok); err == nil {
		t.Fatal("bekor qilingan token qabul qilinmasligi kerak")
	}
}

func TestToken_WrongSecretRejected(t *testing.T) {
	reg := newTestRegistry(t)
	tok, _, _ := reg.Issue("", []string{"http"}, time.Hour)
	other := NewTokenRegistry(filepath.Join(t.TempDir(), "other.json"), []byte("boshqa-sir"))
	if _, err := other.Parse(tok); err == nil {
		t.Fatal("boshqa sir bilan imzo tekshiruvi o'tmasligi kerak")
	}
}

func TestToken_NotInRegistryRejected(t *testing.T) {
	reg := newTestRegistry(t)
	tok, _, _ := reg.Issue("", []string{"http"}, time.Hour)
	// bir xil sir, lekin bo'sh reyestr → imzo to'g'ri, ammo jti yo'q (fail-closed)
	empty := NewTokenRegistry(filepath.Join(t.TempDir(), "empty.json"), []byte("testsecret"))
	if _, err := empty.Parse(tok); err == nil {
		t.Fatal("reyestrda yo'q token qabul qilinmasligi kerak")
	}
}

func TestToken_ExpiredRejected(t *testing.T) {
	reg := newTestRegistry(t)
	tok, _, _ := reg.Issue("", []string{"http"}, 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if _, err := reg.Parse(tok); err == nil {
		t.Fatal("muddati o'tgan token qabul qilinmasligi kerak")
	}
}

// startScopedServer JWT yoqilgan test server'ni qaytaradi.
func startScopedServer(t *testing.T) (*httptest.Server, *TokenRegistry) {
	t.Helper()
	cfg := Config{
		WorkerToken:    "secret",
		APIToken:       "roottoken",
		PingInterval:   50 * time.Millisecond,
		PongWait:       200 * time.Millisecond,
		WaitTimeout:    2 * time.Second,
		TaskTimeout:    500 * time.Millisecond,
		MaxRetries:     2,
		MaxFileSize:    1 << 20,
		MaxMessageSize: 1 << 20,
	}
	store := NewTaskStore()
	hub := NewHub(cfg, store)
	go hub.Run()
	blobs, err := NewBlobStore(t.TempDir(), time.Hour, cfg.MaxFileSize)
	if err != nil {
		t.Fatal(err)
	}
	reg := newTestRegistry(t)
	srv := NewServer(cfg, hub, store, blobs, reg)
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)
	return ts, reg
}

func postTask(t *testing.T, url, token, kind string) int {
	t.Helper()
	body := bytes.NewBufferString(`{"payload":{"kind":"` + kind + `","spec":{}}}`)
	req, _ := http.NewRequest("POST", url+"/tasks", body)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func TestE2E_ScopedTokenEnforcesKind(t *testing.T) {
	ts, reg := startScopedServer(t)
	tok, rec, _ := reg.Issue("", []string{"http"}, time.Hour)

	// ruxsat etilgan kind → 503 (worker yo'q, lekin auth+kind o'tdi — 401/403 emas)
	if code := postTask(t, ts.URL, tok, "http"); code != http.StatusServiceUnavailable {
		t.Fatalf("ruxsat etilgan kind uchun kutilgan 503, olindi %d", code)
	}
	// ruxsat etilmagan kind → 403
	if code := postTask(t, ts.URL, tok, "email"); code != http.StatusForbidden {
		t.Fatalf("ruxsat etilmagan kind uchun kutilgan 403, olindi %d", code)
	}
	// root token → istalgan kind (kind tekshiruvi o'tkazib yuboriladi)
	if code := postTask(t, ts.URL, "roottoken", "email"); code != http.StatusServiceUnavailable {
		t.Fatalf("root token uchun kutilgan 503, olindi %d", code)
	}
	// revoke qilingandan keyin → 401
	if err := reg.Revoke(rec.JTI); err != nil {
		t.Fatal(err)
	}
	if code := postTask(t, ts.URL, tok, "http"); code != http.StatusUnauthorized {
		t.Fatalf("bekor qilingan token uchun kutilgan 401, olindi %d", code)
	}
}

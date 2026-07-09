package main

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"
)

func TestBlobStore_PutOpen(t *testing.T) {
	bs, err := NewBlobStore(t.TempDir(), time.Hour, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := bs.Put("hello.txt", "text/plain", strings.NewReader("salom"))
	if err != nil {
		t.Fatal(err)
	}
	if meta.Size != 5 || meta.Filename != "hello.txt" || meta.ContentType != "text/plain" {
		t.Fatalf("noto'g'ri meta: %+v", meta)
	}
	got, f, err := bs.Open(meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, _ := io.ReadAll(f)
	if string(data) != "salom" || got.ID != meta.ID {
		t.Fatalf("noto'g'ri o'qildi: %q %+v", data, got)
	}
}

func TestBlobStore_TooLarge(t *testing.T) {
	bs, _ := NewBlobStore(t.TempDir(), time.Hour, 4)
	if _, err := bs.Put("big", "", bytes.NewReader([]byte("12345"))); err != ErrBlobTooLarge {
		t.Fatalf("kutilgan ErrBlobTooLarge, olindi %v", err)
	}
}

func TestBlobStore_NotFound(t *testing.T) {
	bs, _ := NewBlobStore(t.TempDir(), time.Hour, 1<<20)
	if _, _, err := bs.Open("yoq-id"); err != ErrBlobNotFound {
		t.Fatalf("kutilgan ErrBlobNotFound, olindi %v", err)
	}
}

func TestBlobStore_GC(t *testing.T) {
	// TTL manfiy → yaratilgan fayl darrov muddati o'tgan hisoblanadi.
	bs, _ := NewBlobStore(t.TempDir(), -time.Second, 1<<20)
	meta, _ := bs.Put("x", "", strings.NewReader("x"))
	bs.GC()
	if _, _, err := bs.Open(meta.ID); err != ErrBlobNotFound {
		t.Fatalf("GC dan keyin fayl qolmasligi kerak, olindi %v", err)
	}
}

func TestSanitizeFilename(t *testing.T) {
	cases := map[string]string{
		"note.txt":         "note.txt",
		"../../etc/passwd": "passwd",
		"a\"b\nc.txt":      "abc.txt",
		"/":                "",
		"":                 "",
	}
	for in, want := range cases {
		if got := sanitizeFilename(in); got != want {
			t.Errorf("sanitizeFilename(%q) = %q, kutilgan %q", in, got, want)
		}
	}
}

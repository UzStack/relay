package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStoreLifecycle(t *testing.T) {
	s := NewTaskStore()
	task := s.Create(json.RawMessage(`{"x":1}`))
	if task.Status != StatusPending {
		t.Fatalf("kutilgan pending, olindi %s", task.Status)
	}

	got, ok := s.Get(task.ID)
	if !ok || got.ID != task.ID {
		t.Fatal("task Get orqali topilmadi")
	}

	s.MarkDispatched(task.ID, "client-1", "10.0.0.5", 1)
	if got, _ := s.Get(task.ID); got.Status != StatusDispatched || got.ClientID != "client-1" || got.ClientIP != "10.0.0.5" {
		t.Fatalf("dispatched holati noto'g'ri: %+v", got)
	}

	s.Finalize(task.ID, StatusDone, json.RawMessage(`{"ok":true}`), "")
	got, _ = s.Get(task.ID)
	if got.Status != StatusDone || string(got.Result) != `{"ok":true}` {
		t.Fatalf("done holati noto'g'ri: %+v", got)
	}
}

func TestFinalizeIsIdempotent(t *testing.T) {
	s := NewTaskStore()
	task := s.Create(nil)
	s.Finalize(task.ID, StatusDone, json.RawMessage(`"birinchi"`), "")
	// kechikkan/takroriy finalize e'tiborsiz qolishi kerak (birinchi g'olib)
	s.Finalize(task.ID, StatusFailed, nil, "kech keldi")
	got, _ := s.Get(task.ID)
	if got.Status != StatusDone || string(got.Result) != `"birinchi"` {
		t.Fatalf("birinchi finalize g'olib bo'lishi kerak edi: %+v", got)
	}
}

func TestWaitWakesOnFinalize(t *testing.T) {
	s := NewTaskStore()
	task := s.Create(nil)

	done := make(chan bool, 1)
	go func() { done <- s.Wait(task, time.Second) }()

	time.Sleep(20 * time.Millisecond)
	s.Finalize(task.ID, StatusDone, json.RawMessage(`"hi"`), "")

	if !<-done {
		t.Fatal("Wait Finalize'dan keyin true qaytarishi kerak edi")
	}
}

func TestWaitTimeout(t *testing.T) {
	s := NewTaskStore()
	task := s.Create(nil)
	if s.Wait(task, 30*time.Millisecond) {
		t.Fatal("Wait timeout'da false qaytarishi kerak edi")
	}
}

func TestEmitDisconnectSignalsOpenTasks(t *testing.T) {
	s := NewTaskStore()
	a := s.Create(nil)
	b := s.Create(nil)
	s.MarkDispatched(a.ID, "c1", "1.1.1.1", 1)
	s.MarkDispatched(b.ID, "c2", "2.2.2.2", 1)

	s.EmitDisconnect("c1")

	// c1 ning task'iga disconnect signali kelishi kerak
	select {
	case ev := <-a.events:
		if !ev.disconnect || ev.clientID != "c1" {
			t.Fatalf("noto'g'ri signal: %+v", ev)
		}
	default:
		t.Fatal("c1 task'iga disconnect signali kelmadi")
	}
	// c2 ga tegmasligi kerak
	select {
	case <-b.events:
		t.Fatal("c2 task'iga signal kelmasligi kerak edi")
	default:
	}
}

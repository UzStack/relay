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

	s.MarkDispatched(task.ID, "client-1")
	if got, _ := s.Get(task.ID); got.Status != StatusDispatched || got.ClientID != "client-1" {
		t.Fatalf("dispatched holati noto'g'ri: %+v", got)
	}

	s.Complete(task.ID, StatusDone, json.RawMessage(`{"ok":true}`), "")
	got, _ = s.Get(task.ID)
	if got.Status != StatusDone || string(got.Result) != `{"ok":true}` {
		t.Fatalf("done holati noto'g'ri: %+v", got)
	}
}

func TestWaitWakesOnComplete(t *testing.T) {
	s := NewTaskStore()
	task := s.Create(nil)

	done := make(chan bool, 1)
	go func() { done <- s.Wait(task, time.Second) }()

	time.Sleep(20 * time.Millisecond)
	s.Complete(task.ID, StatusDone, json.RawMessage(`"hi"`), "")

	if !<-done {
		t.Fatal("Wait Complete'dan keyin true qaytarishi kerak edi")
	}
}

func TestWaitTimeout(t *testing.T) {
	s := NewTaskStore()
	task := s.Create(nil)
	if s.Wait(task, 30*time.Millisecond) {
		t.Fatal("Wait timeout'da false qaytarishi kerak edi")
	}
}

func TestFailTasksForClient(t *testing.T) {
	s := NewTaskStore()
	a := s.Create(nil)
	b := s.Create(nil)
	s.MarkDispatched(a.ID, "c1")
	s.MarkDispatched(b.ID, "c2")

	s.FailTasksForClient("c1")

	if got, _ := s.Get(a.ID); got.Status != StatusFailed || got.Error != "worker_disconnected" {
		t.Fatalf("a fail bo'lishi kerak edi: %+v", got)
	}
	if got, _ := s.Get(b.ID); got.Status != StatusDispatched {
		t.Fatalf("b o'zgarmasligi kerak edi: %+v", got)
	}
}

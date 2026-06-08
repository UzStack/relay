package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// writeWait — bitta yozish uchun maksimal vaqt.
	writeWait = 10 * time.Second
	// sendBuffer — client'ning chiquvchi xabar bufer hajmi.
	sendBuffer = 32
)

// Client bitta ulangan worker'ni ifodalaydi.
type Client struct {
	id   string
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
}

func NewClient(id string, hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		id:   id,
		hub:  hub,
		conn: conn,
		send: make(chan []byte, sendBuffer),
	}
}

// readPump client'dan keladigan xabarlarni o'qiydi va heartbeat deadline'ni boshqaradi.
// Bu goroutine connection yopilguncha ishlaydi.
func (c *Client) readPump() {
	cfg := c.hub.cfg
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadDeadline(time.Now().Add(cfg.PongWait))
	// Har pong kelganda read deadline'ni uzaytiramiz → client active hisoblanadi.
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(cfg.PongWait))
		return nil
	})

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("o'qish xatosi (client %s): %v", c.id, err)
			}
			return
		}
		var msg inMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("noto'g'ri xabar (client %s): %v", c.id, err)
			continue
		}
		if msg.Type == "result" {
			c.hub.handleResult(msg)
		}
	}
}

// writePump send channel'dan xabarlarni yozadi va har PingInterval'da ping yuboradi.
func (c *Client) writePump() {
	cfg := c.hub.cfg
	ticker := time.NewTicker(cfg.PingInterval)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// hub send channel'ni yopdi → connection'ni yopamiz.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Printf("yozish xatosi (client %s): %v", c.id, err)
				return
			}
		case <-ticker.C:
			// Heartbeat: har 2 soniyada ping. Pong kelmasa readPump deadline'da uziladi.
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

package socketio

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
	"math/rand"

	"github.com/gorilla/websocket"
	"github.com/xrpscan/platform/logger"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var (
	hubInstance *Hub
	hubOnce     sync.Once
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type EngineHandshake struct {
	SID          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int      `json:"pingInterval"`
	PingTimeout  int      `json:"pingTimeout"`
}

type ClientConnection struct {
	conn *websocket.Conn
	sid  string
	ns   string
	mu   sync.Mutex
}

type Hub struct {
	clients map[string]*ClientConnection
	mu      sync.RWMutex
}

func GetHub() *Hub {
	hubOnce.Do(func() {
		hubInstance = &Hub{
			clients: make(map[string]*ClientConnection),
		}
	})
	return hubInstance
}

func generateSID() string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 20)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func (h *Hub) HandleSocketIO(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	EIO := r.URL.Query().Get("EIO")
	transport := r.URL.Query().Get("transport")
	sid := r.URL.Query().Get("sid")

	if EIO != "4" {
		http.Error(w, "Only Engine.IO v4 allowed", 400)
		return
	}

	if transport == "polling" {
		sid = generateSID()

		hs := EngineHandshake{
			SID:          sid,
			Upgrades:     []string{"websocket"},
			PingInterval: 25000,
			PingTimeout:  20000,
		}

		js, _ := json.Marshal(hs)
		w.Write([]byte("0" + string(js)))
		return
	}

	if transport == "websocket" {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.Log.Error().Err(err).Msg("upgrade failed")
			return
		}

		if sid == "" {
			sid = generateSID()
		}

		client := &ClientConnection{
			conn: ws,
			sid:  sid,
			ns:   "/",
		}

		logger.Log.Info().Str("sid", sid).Msg("WS connected")

		h.mu.Lock()
		h.clients[sid] = client
		h.mu.Unlock()

		defer func() {
			h.mu.Lock()
			delete(h.clients, sid)
			h.mu.Unlock()
		}()

		hs := EngineHandshake{
			SID:          sid,
			Upgrades:     []string{},
			PingInterval: 25000,
			PingTimeout:  20000,
		}
		js, _ := json.Marshal(hs)

		client.mu.Lock()
		ws.WriteMessage(websocket.TextMessage, []byte("0"+string(js)))
		client.mu.Unlock()

		_, recv, err := ws.ReadMessage()
		if err != nil {
			logger.Log.Error().Err(err).Msg("Client disconnected before CONNECT")
			return
		}

		raw := string(recv)
		logger.Log.Info().Str("msg", raw).Msg("client CONNECT packet")

		if !strings.HasPrefix(raw, "40") {
			logger.Log.Warn().Str("msg", raw).Msg("Invalid CONNECT")
			ws.Close()
			return
		}

		ns := "/"
		if len(raw) > 2 && raw[2] == '/' {
			parts := strings.SplitN(raw[2:], ",", 2)
			ns = parts[0]
		}
		client.ns = ns

		ack := ""
		if ns == "/" {
			ack = fmt.Sprintf(`40{"sid":"%s"}`, sid)
		} else {
			ack = fmt.Sprintf(`40%s,{"sid":"%s"}`, ns, sid)
		}

		client.mu.Lock()
		ws.WriteMessage(websocket.TextMessage, []byte(ack))
		client.mu.Unlock()

		logger.Log.Info().Str("ack", ack).Msg("sent CONNECT ACK")

		go func(c *ClientConnection) {
			for {
				time.Sleep(25 * time.Second)
				c.mu.Lock()
				err := c.conn.WriteMessage(websocket.TextMessage, []byte("2"))
				c.mu.Unlock()
				if err != nil {
					return
				}
			}
		}(client)

		for {
			_, payload, err := ws.ReadMessage()
			if err != nil {
				logger.Log.Info().Str("sid", sid).Msg("client disconnected")
				return
			}
			logger.Log.Info().Str("sid", sid).Str("payload", string(payload)).Msg("received")
		}
	}
}

func (h *Hub) EmitLedgerClosed(event LedgerClosedEvent) {
	h.mu.RLock()
	clients := make([]*ClientConnection, 0, len(h.clients))
	for _, c := range h.clients {
			clients = append(clients, c)
	}
	h.mu.RUnlock()

	if len(clients) == 0 {
			return
	}

	jsonEvent, _ := json.Marshal(event)

	frame := fmt.Sprintf(`42["ledger_closed",%s]`, string(jsonEvent))

	for _, cli := range clients {
			cli.mu.Lock()
			err := cli.conn.WriteMessage(websocket.TextMessage, []byte(frame))
			cli.mu.Unlock()

			if err != nil {
					logger.Log.Error().Err(err).Str("sid", cli.sid).Msg("EmitLedgerClosed failed")
			}
	}
}

func (h *Hub) EmitTransactionProcessed(event TransactionProcessedEvent) {
	h.mu.RLock()
	clients := make([]*ClientConnection, 0, len(h.clients))
	for _, c := range h.clients {
			clients = append(clients, c)
	}
	h.mu.RUnlock()

	if len(clients) == 0 {
			return
	}

	jsonEvent, _ := json.Marshal(event)

	frame := fmt.Sprintf(`42["transaction_processed",%s]`, string(jsonEvent))

	for _, cli := range clients {
			cli.mu.Lock()
			err := cli.conn.WriteMessage(websocket.TextMessage, []byte(frame))
			cli.mu.Unlock()

			if err != nil {
					logger.Log.Error().Err(err).Str("sid", cli.sid).Msg("EmitTransactionProcessed failed")
			}
	}
}
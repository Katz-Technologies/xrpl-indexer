package socketio

import (
	"net/http"
	"sync"

	socketio "github.com/googollee/go-socket.io"
	"github.com/googollee/go-socket.io/engineio"
	"github.com/googollee/go-socket.io/engineio/transport"
	"github.com/googollee/go-socket.io/engineio/transport/polling"
	"github.com/googollee/go-socket.io/engineio/transport/websocket"
	"github.com/xrpscan/platform/logger"
)

var (
	hubInstance *Hub
	hubOnce     sync.Once
)

// Hub manages SocketIO connections and broadcasts events
type Hub struct {
	server *socketio.Server
	mu     sync.RWMutex
}

// GetHub returns the singleton SocketIO hub instance
func GetHub() *Hub {
	hubOnce.Do(func() {
		hubInstance = &Hub{}
		hubInstance.init()
	})
	return hubInstance
}

// init initializes the SocketIO server
func (h *Hub) init() {
	server := socketio.NewServer(&engineio.Options{
		Transports: []transport.Transport{
			&polling.Transport{
				CheckOrigin: allowOriginFunc,
			},
			&websocket.Transport{
				CheckOrigin: allowOriginFunc,
			},
		},
		RequestChecker: allowRequestFunc,
	})

	server.OnConnect("/", func(s socketio.Conn) error {
		logger.Log.Info().Str("id", s.ID()).Msg("SocketIO client connected")
		return nil
	})

	server.OnDisconnect("/", func(s socketio.Conn, reason string) {
		logger.Log.Info().Str("id", s.ID()).Str("reason", reason).Msg("SocketIO client disconnected")
	})

	server.OnError("/", func(s socketio.Conn, e error) {
		logger.Log.Error().Err(e).Str("id", s.ID()).Msg("SocketIO error")
	})

	h.server = server
}

// GetServer returns the SocketIO server instance
func (h *Hub) GetServer() *socketio.Server {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.server
}

// EmitLedgerClosed emits a ledger closed event to all connected clients
func (h *Hub) EmitLedgerClosed(event LedgerClosedEvent) {
	server := h.GetServer()
	if server != nil {
		server.BroadcastToNamespace("/", "ledger_closed", event)
		logger.Log.Debug().
			Uint32("ledger_index", event.LedgerIndex).
			Msg("Emitted ledger_closed event via SocketIO")
	}
}

// EmitTransactionProcessed emits a transaction processed event to all connected clients
func (h *Hub) EmitTransactionProcessed(event TransactionProcessedEvent) {
	server := h.GetServer()
	if server != nil {
		server.BroadcastToNamespace("/", "transaction_processed", event)
		logger.Log.Debug().
			Str("tx_hash", event.Hash).
			Uint32("ledger_index", event.LedgerIndex).
			Msg("Emitted transaction_processed event via SocketIO")
	}
}

// allowOriginFunc allows all origins (can be customized for production)
func allowOriginFunc(r *http.Request) bool {
	return true
}

// allowRequestFunc allows all requests (can be customized for production)
func allowRequestFunc(r *http.Request) (http.Header, error) {
	return nil, nil
}

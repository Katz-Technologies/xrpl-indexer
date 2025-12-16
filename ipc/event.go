package ipc

// Event представляет событие, отправляемое между процессами
type Event struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

// BatchEvent представляет пакетное событие со списком объектов
type BatchEvent struct {
	Type  string        `json:"type"`
	Items []interface{} `json:"items"`
}

// EventType определяет типы событий
const (
	EventTypeData    = "data"
	EventTypeBatch   = "batch"
	EventTypeError   = "error"
	EventTypeReady   = "ready"
	EventTypeDone    = "done"
	EventTypeCommand = "command"
)

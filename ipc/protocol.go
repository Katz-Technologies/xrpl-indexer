package ipc

import (
	"bufio"
	"encoding/json"
	"io"
	"sync"
)

// Protocol обрабатывает JSON-line протокол для IPC
type Protocol struct {
	encoder *json.Encoder
	decoder *json.Decoder
	mu      sync.Mutex
}

// NewProtocol создает новый протокол для указанного потока
func NewProtocol(w io.Writer, r io.Reader) *Protocol {
	return &Protocol{
		encoder: json.NewEncoder(w),
		decoder: json.NewDecoder(r),
	}
}

// Send отправляет событие (оптимизировано для производительности)
func (p *Protocol) Send(event *Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.encoder.Encode(event)
}

// SendBatch отправляет пакетное событие со списком объектов
func (p *Protocol) SendBatch(event *BatchEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.encoder.Encode(event)
}

// Receive читает следующее событие из потока
func (p *Protocol) Receive() (*Event, error) {
	var event Event
	if err := p.decoder.Decode(&event); err != nil {
		return nil, err
	}
	return &event, nil
}

// ReceiveBatch читает пакетное событие
func (p *Protocol) ReceiveBatch() (*BatchEvent, error) {
	var event BatchEvent
	if err := p.decoder.Decode(&event); err != nil {
		return nil, err
	}
	return &event, nil
}

// ScannerProtocol использует bufio.Scanner для более эффективного чтения
type ScannerProtocol struct {
	scanner *bufio.Scanner
	encoder *json.Encoder
	mu      sync.Mutex
}

// NewScannerProtocol создает протокол на основе Scanner (быстрее для больших объемов)
// По умолчанию поддерживает до 50MB для обработки больших пакетов транзакций
func NewScannerProtocol(w io.Writer, r io.Reader) *ScannerProtocol {
	return NewScannerProtocolWithBuffer(w, r, 50*1024*1024) // 50MB по умолчанию
}

// NewScannerProtocolWithBuffer создает протокол с настраиваемым размером буфера
func NewScannerProtocolWithBuffer(w io.Writer, r io.Reader, maxBufferSize int) *ScannerProtocol {
	scanner := bufio.NewScanner(r)
	// Начальный буфер 256KB, максимальный - настраиваемый (по умолчанию 50MB)
	// Это позволяет обрабатывать сотни транзакций XRPL в одном пакете
	buf := make([]byte, 0, 256*1024)
	scanner.Buffer(buf, maxBufferSize)

	return &ScannerProtocol{
		scanner: scanner,
		encoder: json.NewEncoder(w),
	}
}

// Send отправляет событие
func (sp *ScannerProtocol) Send(event *Event) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.encoder.Encode(event)
}

// SendBatch отправляет пакетное событие
func (sp *ScannerProtocol) SendBatch(event *BatchEvent) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.encoder.Encode(event)
}

// Scan читает следующую строку (должна быть валидный JSON)
func (sp *ScannerProtocol) Scan() bool {
	return sp.scanner.Scan()
}

// Event декодирует текущую строку в событие
func (sp *ScannerProtocol) Event() (*Event, error) {
	var event Event
	if err := json.Unmarshal(sp.scanner.Bytes(), &event); err != nil {
		return nil, err
	}
	return &event, nil
}

// BatchEvent декодирует текущую строку в пакетное событие
func (sp *ScannerProtocol) BatchEvent() (*BatchEvent, error) {
	var event BatchEvent
	if err := json.Unmarshal(sp.scanner.Bytes(), &event); err != nil {
		return nil, err
	}
	return &event, nil
}

// DetectEventType определяет тип события по JSON (без полного декодирования)
func (sp *ScannerProtocol) DetectEventType() (string, error) {
	var temp struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(sp.scanner.Bytes(), &temp); err != nil {
		return "", err
	}
	return temp.Type, nil
}

// Err возвращает ошибку сканера
func (sp *ScannerProtocol) Err() error {
	return sp.scanner.Err()
}

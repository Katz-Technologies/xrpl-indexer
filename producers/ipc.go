package producers

import (
	"os"

	"github.com/xrpscan/platform/ipc"
	"github.com/xrpscan/platform/logger"
)

var (
	// Global IPC protocol instance
	globalIPCProtocol *ipc.ScannerProtocol
)

// InitIPC initializes IPC protocol for subprocess mode
func InitIPC() {
	globalIPCProtocol = ipc.NewScannerProtocol(os.Stdout, os.Stdin)
}

// GetIPCProtocol returns the global IPC protocol instance
func GetIPCProtocol() *ipc.ScannerProtocol {
	return globalIPCProtocol
}

// SendTransactionViaIPC sends a transaction via IPC if in subprocess mode
func SendTransactionViaIPC(tx map[string]interface{}) bool {
	if globalIPCProtocol == nil {
		return false
	}

	event := &ipc.Event{
		Type: ipc.EventTypeData,
		Data: tx,
	}

	if err := globalIPCProtocol.Send(event); err != nil {
		logger.Log.Error().Err(err).Msg("Failed to send transaction via IPC")
		return false
	}

	return true
}

// SendBatchTransactionsViaIPC sends a batch of transactions via IPC
func SendBatchTransactionsViaIPC(txs []map[string]interface{}) bool {
	if globalIPCProtocol == nil {
		return false
	}

	// Convert to []interface{}
	items := make([]interface{}, len(txs))
	for i, tx := range txs {
		items[i] = tx
	}

	batchEvent := &ipc.BatchEvent{
		Type:  ipc.EventTypeBatch,
		Items: items,
	}

	if err := globalIPCProtocol.SendBatch(batchEvent); err != nil {
		logger.Log.Error().Err(err).Msg("Failed to send batch transactions via IPC")
		return false
	}

	return true
}

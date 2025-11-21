package socketio

// LedgerClosedEvent represents a ledger closed event
type LedgerClosedEvent struct {
	LedgerIndex uint32 `json:"ledger_index"`
	LedgerHash  string `json:"ledger_hash"`
	TxnCount    uint32 `json:"txn_count"`
	Timestamp   int64  `json:"timestamp"`
}

// TransactionProcessedEvent represents a transaction processed event
type TransactionProcessedEvent struct {
	Hash        string `json:"hash"`
	LedgerIndex uint32 `json:"ledger_index"`
	Type        string `json:"type"`
	Timestamp   int64  `json:"timestamp"`
}

// NewTokenDetectedEvent represents a new token detected event
type NewTokenDetectedEvent struct {
	Currency    string `json:"currency"`
	Issuer      string `json:"issuer"`
	LedgerIndex uint32 `json:"ledger_index"`
	Timestamp   int64  `json:"timestamp"`
}

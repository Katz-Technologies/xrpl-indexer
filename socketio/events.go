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

// MoneyFlowData represents money flow transaction data
type MoneyFlowData struct {
	TxHash       string `json:"tx_hash"`
	LedgerIndex  uint32 `json:"ledger_index"`
	Kind         string `json:"kind"` // 'swap' or 'dexOffer'
	FromAddress  string `json:"from_address"`
	ToAddress    string `json:"to_address"`
	FromCurrency string `json:"from_currency"`
	FromIssuer   string `json:"from_issuer"`
	ToCurrency   string `json:"to_currency"`
	ToIssuer     string `json:"to_issuer"`
	FromAmount   string `json:"from_amount"`
	ToAmount     string `json:"to_amount"`
	Timestamp    int64  `json:"timestamp"`
}

// SubscriptionActivityItem represents a single money flow with its subscribers
type SubscriptionActivityItem struct {
	Subscribers []string      `json:"subscribers"` // массив адресов подписчиков
	MoneyFlow   MoneyFlowData `json:"money_flow"`  // данные транзакции
}

// SubscriptionActivityEvent represents a batch of subscription activities
// Содержит массив транзакций, каждая с массивом подписчиков
type SubscriptionActivityEvent struct {
	Activities []SubscriptionActivityItem `json:"activities"` // массив транзакций с подписчиками
}

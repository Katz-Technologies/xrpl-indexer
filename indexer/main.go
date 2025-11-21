package indexer

import (
	"encoding/json"
	"errors"
	"strconv"
)

func GetLedgerIndex(message []byte) (int, error) {
	// message is in JSON format
	var data map[string]interface{}
	err := json.Unmarshal(message, &data)
	if err != nil {
		return 0, err
	}

	// Ledger stream encodes ledger_index as a number
	ledgerIndexL, ok := data["ledger_index"].(float64)
	if ok {
		return int(ledgerIndexL), nil
	}

	// Validation stream encodes ledger_index as a string
	ledgerIndexV, ok := data["ledger_index"].(string)
	if ok {
		li, err := strconv.Atoi(ledgerIndexV)
		if err != nil {
			return 0, err
		}
		return li, nil
	}

	// Return error if ledger_index is encoded in an unknown type
	return 0, errors.New("GetLedgerIndex: ledger_index not found")
}

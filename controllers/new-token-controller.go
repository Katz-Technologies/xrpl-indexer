package controllers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/xrpscan/platform/connections"
)

type NewToken struct {
	CurrencyCode           string `json:"currency_code"`
	Issuer                 string `json:"issuer"`
	FirstSeenLedgerIndex   uint32 `json:"first_seen_ledger_index"`
	FirstSeenInLedgerIndex uint32 `json:"first_seen_in_ledger_index"`
}

func GetNewTokens(c echo.Context) error {
	limitStr := c.QueryParam("limit")
	offsetStr := c.QueryParam("offset")

	if limitStr == "" || offsetStr == "" {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"status":  http.StatusBadRequest,
			"message": "limit and offset query parameters are required",
		})
	}

	var limit, offset int
	if _, err := fmt.Sscanf(limitStr, "%d", &limit); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"status":  http.StatusBadRequest,
			"message": fmt.Sprintf("Invalid limit parameter: %v", err),
		})
	}
	if _, err := fmt.Sscanf(offsetStr, "%d", &offset); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"status":  http.StatusBadRequest,
			"message": fmt.Sprintf("Invalid offset parameter: %v", err),
		})
	}

	query := fmt.Sprintf(`
		SELECT currency_code, issuer, first_seen_ledger_index, first_seen_in_ledger_index 
		FROM xrpl.new_tokens 
		ORDER BY first_seen_ledger_index, first_seen_in_ledger_index 
		LIMIT %d OFFSET %d
	`, limit, offset)

	rows, err := connections.ClickHouseConn.Query(context.Background(), query)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"status":  http.StatusInternalServerError,
			"message": fmt.Sprintf("Failed to get new tokens: %v", err),
		})
	}
	defer rows.Close()

	var tokens []NewToken
	for rows.Next() {
		var token NewToken
		err := rows.Scan(&token.CurrencyCode, &token.Issuer, &token.FirstSeenLedgerIndex, &token.FirstSeenInLedgerIndex)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]interface{}{
				"status":  http.StatusInternalServerError,
				"message": fmt.Sprintf("Failed to scan new tokens: %v", err),
			})
		}
		tokens = append(tokens, token)
	}

	if err := rows.Err(); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"status":  http.StatusInternalServerError,
			"message": fmt.Sprintf("Error iterating rows: %v", err),
		})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"status":  http.StatusOK,
		"message": "New tokens fetched successfully",
		"data":    tokens,
	})
}

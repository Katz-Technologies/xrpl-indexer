package controllers

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/logger"
)

// SubscriptionLinkRequest структура для запроса создания/удаления подписки
type SubscriptionLinkRequest struct {
	FromAddress string `json:"from_address" validate:"required"`
	ToAddress   string `json:"to_address" validate:"required"`
}

// CreateSubscriptionLink создает связку подписки
// POST /subscription-links
// Body: {"from_address": "rXXX...", "to_address": "rYYY..."}
func CreateSubscriptionLink(c echo.Context) error {
	var req SubscriptionLinkRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"status":  http.StatusBadRequest,
			"message": fmt.Sprintf("Invalid request body: %v", err),
		})
	}

	if req.FromAddress == "" || req.ToAddress == "" {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"status":  http.StatusBadRequest,
			"message": "from_address and to_address are required",
		})
	}

	if connections.ClickHouseConn == nil {
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"status":  http.StatusInternalServerError,
			"message": "ClickHouse connection not initialized",
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
		INSERT INTO xrpl.subscription_links 
		(from_address, to_address)
		VALUES (?, ?)
	`

	err := connections.ClickHouseConn.Exec(ctx, query, req.FromAddress, req.ToAddress)
	if err != nil {
		logger.Log.Error().
			Err(err).
			Str("from_address", req.FromAddress).
			Str("to_address", req.ToAddress).
			Msg("Failed to create subscription link")
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"status":  http.StatusInternalServerError,
			"message": fmt.Sprintf("Failed to create subscription link: %v", err),
		})
	}

	logger.Log.Info().
		Str("from_address", req.FromAddress).
		Str("to_address", req.ToAddress).
		Msg("Subscription link created successfully")

	return c.JSON(http.StatusOK, map[string]interface{}{
		"status":  http.StatusOK,
		"message": "Subscription link created successfully",
		"data": map[string]string{
			"from_address": req.FromAddress,
			"to_address":   req.ToAddress,
		},
	})
}

// DeleteSubscriptionLink удаляет связку подписки
// DELETE /subscription-links
// Body: {"from_address": "rXXX...", "to_address": "rYYY..."}
func DeleteSubscriptionLink(c echo.Context) error {
	var req SubscriptionLinkRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"status":  http.StatusBadRequest,
			"message": fmt.Sprintf("Invalid request body: %v", err),
		})
	}

	if req.FromAddress == "" || req.ToAddress == "" {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"status":  http.StatusBadRequest,
			"message": "from_address and to_address are required",
		})
	}

	if connections.ClickHouseConn == nil {
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"status":  http.StatusInternalServerError,
			"message": "ClickHouse connection not initialized",
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
		ALTER TABLE xrpl.subscription_links 
		DELETE WHERE from_address = ? AND to_address = ?
	`

	err := connections.ClickHouseConn.Exec(ctx, query, req.FromAddress, req.ToAddress)
	if err != nil {
		logger.Log.Error().
			Err(err).
			Str("from_address", req.FromAddress).
			Str("to_address", req.ToAddress).
			Msg("Failed to delete subscription link")
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"status":  http.StatusInternalServerError,
			"message": fmt.Sprintf("Failed to delete subscription link: %v", err),
		})
	}

	logger.Log.Info().
		Str("from_address", req.FromAddress).
		Str("to_address", req.ToAddress).
		Msg("Subscription link deleted successfully")

	return c.JSON(http.StatusOK, map[string]interface{}{
		"status":  http.StatusOK,
		"message": "Subscription link deleted successfully",
		"data": map[string]string{
			"from_address": req.FromAddress,
			"to_address":   req.ToAddress,
		},
	})
}

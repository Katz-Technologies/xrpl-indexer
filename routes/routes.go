package routes

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/xrpscan/platform/controllers"
	"github.com/xrpscan/platform/socketio"
)

func Add(e *echo.Echo) {
	// Subscription links
	e.POST("/subscription-links", controllers.CreateSubscriptionLink)
	e.DELETE("/subscription-links", controllers.DeleteSubscriptionLink)

	e.GET("/new-tokens", controllers.GetNewTokens)

	hub := socketio.GetHub()

	e.Any("/socket.io/", echo.WrapHandler(http.HandlerFunc(hub.HandleSocketIO)))
	e.Any("/socket.io/*", echo.WrapHandler(http.HandlerFunc(hub.HandleSocketIO)))

	// Health check
	e.GET("/socketio/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]interface{}{
			"status":  "ok",
			"service": "socketio",
		})
	})
}

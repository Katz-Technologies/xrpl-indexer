package routes

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/xrpscan/platform/controllers"
	"github.com/xrpscan/platform/socketio"
)

func Add(e *echo.Echo) {
	e.GET("/tx/:hash", controllers.GetTransaction)
	e.GET("/account/:address", controllers.GetAccountInfo)

	// SocketIO endpoint - must be registered before other routes that might match
	// SocketIO handles its own path routing, so we use Any to catch all methods
	socketIOServer := socketio.GetHub().GetServer()
	e.Any("/socket.io/", echo.WrapHandler(socketIOServer))
	e.Any("/socket.io/*", echo.WrapHandler(socketIOServer))

	// Health check endpoint for SocketIO (before SocketIO routes to avoid conflicts)
	e.GET("/socketio/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]interface{}{
			"status":  "ok",
			"service": "socketio",
		})
	})
}

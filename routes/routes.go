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

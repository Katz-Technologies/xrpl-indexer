package main

import (
	"fmt"

	"github.com/labstack/echo/v4"
	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/producers"
	"github.com/xrpscan/platform/routes"
	"github.com/xrpscan/platform/signals"
	"github.com/xrpscan/platform/socketio"
)

func main() {
	config.EnvLoad()
	logger.New()

	connections.NewClickHouseConnection()
	connections.NewXrplClient()
	connections.NewXrplRPCClient()

	// Initialize SocketIO hub
	socketio.GetHub()

	go connections.SubscribeStreams()
	go connections.MonitorXRPLConnection()
	go producers.RunProducers()
	// Consumers are no longer needed - transactions are processed directly in producers

	e := echo.New()
	e.HideBanner = true
	routes.Add(e)

	signals.HandleAll()
	serverAddress := fmt.Sprintf("%s:%s", config.EnvServerHost(), config.EnvServerPort())
	e.Logger.Fatal(e.Start(serverAddress))
}

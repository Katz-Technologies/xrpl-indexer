package main

import (
	"fmt"

	"github.com/labstack/echo/v4"
	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/consumers"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/producers"
	"github.com/xrpscan/platform/routes"
	"github.com/xrpscan/platform/signals"
)

func main() {
	config.EnvLoad()
	logger.New()

	connections.NewWriter()
	connections.NewReaders()
	connections.NewXrplClient()
	connections.NewXrplRPCClient()

	go connections.SubscribeStreams()
	go connections.MonitorXRPLConnection()
	go producers.RunProducers()
	go consumers.RunConsumers()

	e := echo.New()
	e.HideBanner = true
	routes.Add(e)

	signals.HandleAll()
	serverAddress := fmt.Sprintf("%s:%s", config.EnvServerHost(), config.EnvServerPort())
	e.Logger.Fatal(e.Start(serverAddress))
}

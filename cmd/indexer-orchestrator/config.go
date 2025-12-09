package main

import (
	"flag"
	"fmt"
	"strings"
	"time"
)

type Config struct {
	Workers       int
	ConfigFile    string
	ServerPath    string
	CheckInterval time.Duration
	LogFile       string
	Servers       []string
}

func ParseConfig() (*Config, error) {
	cfg := &Config{}

	flag.IntVar(&cfg.Workers, "workers", 2, "Number of indexer processes to run in parallel")
	flag.StringVar(&cfg.ConfigFile, "config", ".env", "Environment config file")
	flag.StringVar(&cfg.ServerPath, "server-path", "./bin/platform-server", "Path to platform-server executable")
	flag.DurationVar(&cfg.CheckInterval, "check-interval", 10*time.Second, "Interval to check worker status")
	flag.StringVar(&cfg.LogFile, "log-file", "logs/indexer-orchestrator.log", "Path to orchestrator log file")

	serversStr := flag.String("servers", "wss://s1.ripple.com/,wss://s2.ripple.com/,wss://xrplcluster.com/",
		"Comma-separated list of XRPL server URLs (default: s1, s2, and xrplcluster)")

	flag.Parse()

	// Parse servers
	if *serversStr != "" {
		cfg.Servers = strings.Split(*serversStr, ",")
		for i, s := range cfg.Servers {
			cfg.Servers[i] = strings.TrimSpace(s)
		}
	} else {
		return nil, fmt.Errorf("at least one server must be specified")
	}

	if len(cfg.Servers) == 0 {
		return nil, fmt.Errorf("at least one server must be specified")
	}

	if cfg.Workers <= 0 {
		return nil, fmt.Errorf("workers must be greater than 0")
	}

	return cfg, nil
}

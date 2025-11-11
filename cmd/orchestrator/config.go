package main

import (
	"flag"
	"fmt"
	"strings"
	"time"
)

type Config struct {
	Workers               int
	FromLedger            int
	ToLedger              int
	Servers               []string
	CheckInterval         time.Duration
	ConfigFile            string
	CLIPath               string
	Verbose               bool
	MinDelay              int64
	LogFile               string
	RedistributeThreshold int // If other workers have less than this many ledgers left, let them finish
}

func ParseConfig() (*Config, error) {
	cfg := &Config{}

	flag.IntVar(&cfg.Workers, "workers", 2, "Number of backfill workers to run in parallel")
	flag.IntVar(&cfg.FromLedger, "from", 82000000, "From ledger index")
	flag.IntVar(&cfg.ToLedger, "to", 82001000, "To ledger index")
	flag.StringVar(&cfg.ConfigFile, "config", ".env", "Environment config file")
	flag.StringVar(&cfg.CLIPath, "cli-path", "./bin/platform-cli", "Path to platform-cli executable")
	flag.DurationVar(&cfg.CheckInterval, "check-interval", 30*time.Second, "Interval to check worker status")
	flag.BoolVar(&cfg.Verbose, "verbose", false, "Make the orchestrator more talkative")
	flag.Int64Var(&cfg.MinDelay, "delay", 10, "Minimum delay (ms) between requests to XRPL server")
	flag.StringVar(&cfg.LogFile, "log-file", "", "Path to orchestrator log file (default: stdout)")
	flag.IntVar(&cfg.RedistributeThreshold, "redistribute-threshold", 1000, "If other workers have less than this many ledgers left, let them finish instead of stopping (0 = always stop and redistribute)")

	serversStr := flag.String("servers", "wss://s1.ripple.com/,wss://s2.ripple.com/,wss://xrplcluster.com/",
		"Comma-separated list of XRPL server URLs")

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

	if cfg.FromLedger > cfg.ToLedger {
		return nil, fmt.Errorf("from ledger (%d) must be less than to ledger (%d)", cfg.FromLedger, cfg.ToLedger)
	}

	return cfg, nil
}

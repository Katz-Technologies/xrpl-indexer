package main

import (
	"errors"
	"fmt"
	"os"
)

// Command runner interface
type Runner interface {
	Init([]string) error
	Validate() error
	Name() string
	Run() error
}

// command line root
func root(args []string) error {
	if len(args) < 1 {
		return errors.New("you must pass a sub command")
	}
	subcommand := os.Args[1]

	cmds := []Runner{
		NewBackfillCommand(),
		NewImportXrplmetaTokensCommand(),
	}

	for _, cmd := range cmds {
		if cmd.Name() == subcommand {
			cmd.Init(os.Args[2:])
			if err := cmd.Validate(); err != nil {
				return err
			}

			return cmd.Run()
		}
	}

	return fmt.Errorf("unknown subcommand: %s", subcommand)
}

func main() {
	// Recover from panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "Panic: %v\n", r)
			os.Exit(1)
		}
	}()

	if err := root(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

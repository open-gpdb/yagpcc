package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/open-gpdb/yagpcc/internal/app"
	"github.com/spf13/pflag"
)

const (
	flagNameConfigPath = "config-path"
)

var (
	configPathValue *string
)

const configFile string = "yagpcc.yaml"

func registerConfigPathFlag(set *pflag.FlagSet) {
	configPathValue = set.String(flagNameConfigPath, "", "Path where to look for configuration files")
}

func main() {
	ctxC, ctxCancelF := context.WithCancel(context.Background())

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			// sig is a ^C, handle it
			fmt.Printf("got signal %v - exiting \n", sig)
			ctxCancelF()
			os.Exit(1)
		}
	}()

	registerConfigPathFlag(pflag.CommandLine)
	pflag.Parse()

	for {
		err := app.Run(ctxC, fmt.Sprintf("%s/%s", *configPathValue, configFile))
		if err != nil {
			fmt.Println(err)
		}
		select {
		case <-ctxC.Done():
			os.Exit(1)
		default:
			time.Sleep(time.Second * 1)
		}
	}
}

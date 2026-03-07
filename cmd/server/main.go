package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/open-gpdb/yagpcc/internal/app"
)

const configFile string = "yagpcc.yaml"

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

	for {
		err := app.Run(ctxC, configFile)
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

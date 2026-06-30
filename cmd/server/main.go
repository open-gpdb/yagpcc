// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright
// ownership. The ASF licenses this file to You under the Apache
// License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the
// License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
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
		configPath := filepath.Join(*configPathValue, configFile)
		err := app.Run(ctxC, configPath)
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

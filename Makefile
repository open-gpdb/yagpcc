# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed
# with this work for additional information regarding copyright
# ownership. The ASF licenses this file to You under the Apache
# License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the
# License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

GIT_REVISION=`git rev-parse --short HEAD`
YAGPCC_VERSION=`git describe --tags --abbrev=0`
GOFMT_FILES?=$$(find . -name '*.go' | grep -v .git | grep -v parser | grep -v vendor)

UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
	GOFLAGS := -ldflags=-extldflags=-Wl
else
	GOFLAGS :=
endif

GINKGO_CLI=github.com/onsi/ginkgo/v2/ginkgo@v2.28.0
PROTOC_GEN_GO=google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
PROTOC_GEN_GO_GRPC=google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1

####################### BUILD #######################

genproto:
	go install $(PROTOC_GEN_GO)
	go install $(PROTOC_GEN_GO_GRPC)
	buf generate

genbin:
	mkdir -p devbin

build: genproto genbin
	go build -pgo=auto -o devbin/yagpcc $(GOFLAGS) ./cmd/server

####################### UI BUILD #######################

build-ui:
	cd web && npm ci && npm run build
	rm -rf internal/httpui/dist
	cp -r web/dist internal/httpui/dist

build-all: genproto build-ui genbin
	go build -pgo=auto -o devbin/yagpcc $(GOFLAGS) ./cmd/server

####################### TESTS #######################

unittest:
	go run $(GINKGO_CLI) run --race --github-output ./...

####################### LINTERS #######################

fmt:
	gofmt -w $(GOFMT_FILES)

lint:
	golangci-lint run --timeout=10m

version = $(shell git describe --tags --abbrev=0)
package:
	sed -i 's/YAGPCC_VERSION/${version}/g' debian/changelog
	dpkg-buildpackage -us -uc

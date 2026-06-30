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
BUF_CLI=github.com/bufbuild/buf/cmd/buf@v1.71.0

# Ensure go-installed tools (protoc-gen-go, protoc-gen-go-grpc, buf) are on PATH.
export PATH := $(shell go env GOPATH)/bin:$(PATH)

####################### BUILD #######################

genproto:
	go install $(PROTOC_GEN_GO)
	go install $(PROTOC_GEN_GO_GRPC)
	go install $(BUF_CLI)
	buf generate

genbin:
	mkdir -p devbin

build: genproto genbin
	go build -pgo=auto -o devbin/yagpcc $(GOFLAGS) ./cmd/server

####################### UI BUILD #######################

MIN_NODE_VERSION := 18

check-node:
	@NODE_VER=$$(node -v 2>/dev/null | sed 's/^v//' | cut -d. -f1); \
	if [ -z "$$NODE_VER" ]; then \
		echo "ERROR: Node.js is not installed. Node.js >= $(MIN_NODE_VERSION) is required for building the UI."; \
		echo "Install it from https://nodejs.org/ or via nvm: nvm install $(MIN_NODE_VERSION)"; \
		exit 1; \
	elif [ "$$NODE_VER" -lt $(MIN_NODE_VERSION) ]; then \
		echo "ERROR: Node.js >= $(MIN_NODE_VERSION) is required, but found $$(node -v)."; \
		echo "Please upgrade Node.js. If using nvm: nvm install $(MIN_NODE_VERSION) && nvm use $(MIN_NODE_VERSION)"; \
		exit 1; \
	fi

build-ui: check-node
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

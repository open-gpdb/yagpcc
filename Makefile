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

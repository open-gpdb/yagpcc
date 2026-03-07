GIT_REVISION=`git rev-parse --short HEAD`
YAGPCC_VERSION=`git describe --tags --abbrev=0`
GOFMT_FILES?=$$(find . -name '*.go' | grep -v .git | grep -v parser | grep -v vendor)
GOFLAGS=-ldflags=-extldflags=-Wl,-ld_classic

####################### BUILD #######################

genproto:
	protoc --proto_path=. --go_out=. --go_opt=paths=source_relative api/proto/common/yagpcc_metrics.proto
	protoc --proto_path=. --go_out=. --go_opt=paths=source_relative api/proto/common/yagpcc_session.proto
	protoc --proto_path=. --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative api/proto/agent_segment/yagpcc_control_service.proto
	protoc --proto_path=. --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative api/proto/agent_segment/yagpcc_get_service.proto
	protoc --proto_path=. --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative api/proto/agent_segment/yagpcc_set_service.proto
	protoc --proto_path=. --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative api/proto/agent_master/yagpcc_action_service.proto
	protoc --proto_path=. --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative api/proto/agent_master/yagpcc_get_service.proto

genbin:
	mkdir -p devbin

build: genproto genbin
	go build -pgo=auto -o devbin/yagpcc $(LDFLAGS) ./cmd/server

####################### TESTS #######################

unittest:
	go test -race ./...

####################### LINTERS #######################

fmt:
	gofmt -w $(GOFMT_FILES)

lint:
	golangci-lint run --timeout=10m

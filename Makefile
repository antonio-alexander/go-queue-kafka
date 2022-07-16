## ----------------------------------------------------------------------
## This makefile can be used to execute common functions to interact with
## the source code, these functions ease local development and can also be
## used in CI/CD pipelines.
## ----------------------------------------------------------------------

# REFERENCE: https://stackoverflow.com/questions/16931770/makefile4-missing-separator-stop
help: ## - Show this help.
	@sed -ne '/@sed/!s/## //p' $(MAKEFILE_LIST)

check-proto: ## - check protoc/proto-gen-go/protolint
	 @which protolint || (go install github.com/yoheimuta/protolint/cmd/protolint@v0.38.3)
	 @which protoc || echo protocv3.20.1 not installed, install https://github.com/protocolbuffers/protobuf/releases/tag/v3.20.1
	 @which protoc-gen-go || (go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.0)

lint-proto: check-proto ## - lint proto
	 protolint -fix ./protos/go_queue_kafka.proto

clean-proto: ## - clean protos
	 rm ./protos/go_queue_kafka.pb.go

build-proto: lint-proto ## - build proto
	 protoc -I="./protos" --go_opt=paths=source_relative --go_out="./protos" ./protos/go_queue_kafka.proto

check-lint: ## - validate/install golangci-lint installation
	@which golangci-lint || (go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.44.2)

lint: check-lint ## - lint the source
	golangci-lint run

lint-verbose: check-lint ## - lint the source with verbose output
	golangci-lint run --verbose

check-godoc: ## - validate/install godoc
	@which godoc || (go install golang.org/x/tools/cmd/godoc@v0.1.10)

serve-godoc: check-godoc ## - serve (web) the godocs
	godoc -http :8080

test: ## - test the source
	 go test --count=1 -p 1 -cover --count=1 ./...

test-verbose: ## - test the source with verbose output
	 go test --count=1 -p 1 -cover -v --count=1 ./...

run: ## - run the dependencies
	 docker compose up -d

stop: ## - stop the dependencies
	 docker compose down --volumes
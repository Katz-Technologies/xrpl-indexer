SERVICE_NAME=platform
BIN_DIR=bin
MKDIR=mkdir -p

all: build

build:
	${MKDIR} ${BIN_DIR}
	go build -o ${BIN_DIR}/${SERVICE_NAME}-server main.go
	go build -o ${BIN_DIR}/${SERVICE_NAME}-cli cmd/cli/*.go
	go build -o ${BIN_DIR}/${SERVICE_NAME}-orchestrator cmd/orchestrator/*.go
	go build -o ${BIN_DIR}/${SERVICE_NAME}-indexer-orchestrator cmd/indexer-orchestrator/*.go

clean:
	go clean
	rm -rf ${BIN_DIR}

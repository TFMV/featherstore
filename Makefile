.PHONY: all build test clean run docker-build docker-run setup-dev help

# Project variables
BINARY_NAME=featherstore
VERSION=0.1.0
BUILD_DIR=./bin
MAIN_PACKAGE=./cmd/featherstore

# Go variables
GO=go
GOFLAGS=-v
CGO_ENABLED=1
LDFLAGS=-ldflags "-X main.Version=$(VERSION)"

# Docker variables
DOCKER_IMAGE_NAME=featherstore
DOCKER_TAG=latest

all: clean build test

help:
	@echo "FeatherStore Makefile Help"
	@echo "-------------------------------------------------------------------------"
	@echo "make              : Build, test and create binary"
	@echo "make build        : Build the application"
	@echo "make test         : Run tests"
	@echo "make clean        : Remove build artifacts"
	@echo "make run          : Run the application locally"
	@echo "make docker-build : Build Docker image"
	@echo "make docker-run   : Run application in Docker container"
	@echo "make setup-dev    : Install development dependencies"
	@echo "make lint         : Run linters"
	@echo "make fmt          : Format Go code"
	@echo "make help         : Show this help message"
	@echo "-------------------------------------------------------------------------"

setup-dev:
	$(GO) install golang.org/x/lint/golint@latest
	$(GO) install golang.org/x/tools/cmd/goimports@latest
	$(GO) mod download
	@echo "Development environment setup complete"

build:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(MAIN_PACKAGE)
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

test:
	$(GO) test -race ./...

clean:
	$(GO) clean
	rm -rf $(BUILD_DIR)
	rm -rf ./data

run:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run $(GOFLAGS) $(MAIN_PACKAGE)

docker-build:
	docker build -t $(DOCKER_IMAGE_NAME):$(DOCKER_TAG) .
	@echo "Docker image built: $(DOCKER_IMAGE_NAME):$(DOCKER_TAG)"

docker-run:
	docker run -p 8080:8080 -p 8081:8081 -p 9090:9090 $(DOCKER_IMAGE_NAME):$(DOCKER_TAG)

lint:
	golint ./...
	go vet ./...

fmt:
	goimports -w .
	go fmt ./... 
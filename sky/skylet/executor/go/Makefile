SHELL := /bin/bash
MODULE  := skypilot.dev/executor
PROTO   := proto/agent.proto
GEN_DIR := gen/agent
SKYLET_PROTO   := proto/skylet.proto
SKYLET_GEN_DIR := gen/skylet
BIN_DIR := ../bin

TARGETS := sky-exec sky-agent

.PHONY: all proto deps build clean

all: proto deps build

# --- protobuf codegen --------------------------------------------------------
proto: $(GEN_DIR)/agent.pb.go $(SKYLET_GEN_DIR)/skylet.pb.go

$(GEN_DIR)/agent.pb.go: $(PROTO)
	@mkdir -p $(GEN_DIR)
	protoc \
		--proto_path=proto \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO)
	@echo "agent proto generated"

$(SKYLET_GEN_DIR)/skylet.pb.go: $(SKYLET_PROTO)
	@mkdir -p $(SKYLET_GEN_DIR)
	protoc \
		--proto_path=proto \
		--go_out=$(SKYLET_GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(SKYLET_GEN_DIR) --go-grpc_opt=paths=source_relative \
		$(SKYLET_PROTO)
	@echo "skylet proto generated"

# --- dependencies -------------------------------------------------------------
deps:
	go mod tidy

# --- local build (host arch) --------------------------------------------------
build: proto deps
	@mkdir -p $(BIN_DIR)
	$(foreach t,$(TARGETS), \
		go build -o $(BIN_DIR)/$(t) ./cmd/$(t); \
	)
	@echo "Built: $(addprefix $(BIN_DIR)/,$(TARGETS))"


clean:
	rm -rf $(GEN_DIR) $(BIN_DIR)/sky-exec $(BIN_DIR)/sky-agent

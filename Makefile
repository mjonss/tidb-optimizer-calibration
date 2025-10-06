.PHONY: build clean test help create-table test-optimizer

# Default target
help:
	@echo "Available targets:"
	@echo "  build           - Build the tidb-optimizer-calibration binary"
	@echo "  clean           - Remove built binary"
	@echo "  fmt             - Format Go code"
	@echo "  vet             - Run go vet"
	@echo "  create-table    - Create test table (requires TiDB running)"
	@echo "  test-optimizer  - Run optimizer tests (requires test table)"
	@echo "  help            - Show this help message"

build:
	go build -o tidb-optimizer-calibration .

clean:
	rm -f tidb-optimizer-calibration

fmt:
	go fmt ./...

vet:
	go vet ./...

# Requires TiDB to be running
create-table:
	./tidb-optimizer-calibration -create-table -dsn "$(DSN)"

# Requires test table to exist
test-optimizer:
	./tidb-optimizer-calibration -test-optimizer -dsn "$(DSN)"

# Default DSN if not specified
DSN ?= root@tcp(127.0.0.1:4000)/test

# Makefile for building C FFI examples
#
# Usage:
#   make              - Build the C example
#   make run          - Build and run the C example
#   make clean        - Clean build artifacts

# Detect OS
UNAME_S := $(shell uname -s)

# Library name and paths
LIB_NAME = als_compression
LIB_DIR = ../target/release
INCLUDE_DIR = ../include

# Compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -I$(INCLUDE_DIR)
LDFLAGS = -L$(LIB_DIR) -l$(LIB_NAME) -lpthread -ldl -lm

# Platform-specific settings
ifeq ($(UNAME_S),Linux)
    LIB_EXT = so
    LIB_PATH_VAR = LD_LIBRARY_PATH
endif
ifeq ($(UNAME_S),Darwin)
    LIB_EXT = dylib
    LIB_PATH_VAR = DYLD_LIBRARY_PATH
endif
ifeq ($(OS),Windows_NT)
    LIB_EXT = dll
    LIB_PATH_VAR = PATH
    LDFLAGS = -L$(LIB_DIR) -l$(LIB_NAME)
endif

# Targets
.PHONY: all clean run lib

all: c_example

# Build the Rust library first
lib:
	@echo "Building Rust library with FFI support..."
	cd .. && cargo build --release --features ffi

# Build the C example
c_example: c_example.c lib
	@echo "Building C example..."
	$(CC) $(CFLAGS) -o c_example c_example.c $(LDFLAGS)
	@echo "Build complete!"
	@echo ""
	@echo "To run the example:"
	@echo "  $(LIB_PATH_VAR)=$(LIB_DIR) ./c_example"
	@echo ""
	@echo "Or use: make run"

# Run the example
run: c_example
	@echo "Running C example..."
	@echo ""
	$(LIB_PATH_VAR)=$(LIB_DIR) ./c_example

# Clean build artifacts
clean:
	rm -f c_example
	@echo "Cleaned build artifacts"

# Help
help:
	@echo "Available targets:"
	@echo "  make          - Build the C example"
	@echo "  make run      - Build and run the C example"
	@echo "  make clean    - Clean build artifacts"
	@echo "  make help     - Show this help message"

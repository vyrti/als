#!/bin/bash

# Test script to verify round-trip compression for all datasets
# Each dataset must compress and decompress back to the original with matching hash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Build the CLI tool first
echo "Building ALS CLI tool..."
cargo build --release --manifest-path app/cli/Cargo.toml

CLI_PATH="./target/release/als"

if [ ! -f "$CLI_PATH" ]; then
    echo -e "${RED}Error: CLI binary not found at $CLI_PATH${NC}"
    exit 1
fi

echo -e "${GREEN}CLI tool built successfully${NC}\n"

# Create temporary directory for test outputs
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

echo "Testing round-trip compression for all datasets..."
echo "=================================================="
echo ""

TOTAL=0
PASSED=0
FAILED=0
FAILED_FILES=()

# Test each dataset file
for dataset in datasets/*; do
    # Skip if not a file
    if [ ! -f "$dataset" ]; then
        continue
    fi
    
    # Skip LICENSE file
    if [[ "$dataset" == "datasets/LICENSE" ]]; then
        continue
    fi
    
    TOTAL=$((TOTAL + 1))
    filename=$(basename "$dataset")
    
    echo -n "Testing $filename... "
    
    # Calculate original hash
    if command -v sha256sum &> /dev/null; then
        ORIGINAL_HASH=$(sha256sum "$dataset" | awk '{print $1}')
    elif command -v shasum &> /dev/null; then
        ORIGINAL_HASH=$(shasum -a 256 "$dataset" | awk '{print $1}')
    else
        echo -e "${RED}SKIP${NC} (no hash tool available)"
        continue
    fi
    
    # Determine format based on extension
    FORMAT="auto"
    if [[ "$filename" == *.csv ]]; then
        FORMAT="csv"
    elif [[ "$filename" == *.json ]]; then
        FORMAT="json"
    elif [[ "$filename" == *.log ]]; then
        # Log files are treated as plain text, skip them
        echo -e "${YELLOW}SKIP${NC} (log file)"
        continue
    fi
    
    # Compress
    COMPRESSED="$TEMP_DIR/${filename}.als"
    if ! "$CLI_PATH" compress -i "$dataset" -o "$COMPRESSED" -f "$FORMAT" --quiet 2>&1; then
        echo -e "${RED}FAIL${NC} (compression failed)"
        FAILED=$((FAILED + 1))
        FAILED_FILES+=("$filename (compression)")
        continue
    fi
    
    # Decompress
    DECOMPRESSED="$TEMP_DIR/${filename}.decompressed"
    if ! "$CLI_PATH" decompress -i "$COMPRESSED" -o "$DECOMPRESSED" -f "$FORMAT" --quiet 2>&1; then
        echo -e "${RED}FAIL${NC} (decompression failed)"
        FAILED=$((FAILED + 1))
        FAILED_FILES+=("$filename (decompression)")
        continue
    fi
    
    # Calculate decompressed hash
    if command -v sha256sum &> /dev/null; then
        DECOMPRESSED_HASH=$(sha256sum "$DECOMPRESSED" | awk '{print $1}')
    else
        DECOMPRESSED_HASH=$(shasum -a 256 "$DECOMPRESSED" | awk '{print $1}')
    fi
    
    # Compare hashes
    if [ "$ORIGINAL_HASH" == "$DECOMPRESSED_HASH" ]; then
        # Calculate compression ratio
        ORIGINAL_SIZE=$(wc -c < "$dataset")
        COMPRESSED_SIZE=$(wc -c < "$COMPRESSED")
        RATIO=$(echo "scale=2; $ORIGINAL_SIZE / $COMPRESSED_SIZE" | bc)
        
        echo -e "${GREEN}PASS${NC} (ratio: ${RATIO}x)"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}FAIL${NC} (hash mismatch)"
        echo "  Original:     $ORIGINAL_HASH"
        echo "  Decompressed: $DECOMPRESSED_HASH"
        FAILED=$((FAILED + 1))
        FAILED_FILES+=("$filename (hash mismatch)")
    fi
done

echo ""
echo "=================================================="
echo "Test Results:"
echo "  Total:  $TOTAL"
echo -e "  ${GREEN}Passed: $PASSED${NC}"
if [ $FAILED -gt 0 ]; then
    echo -e "  ${RED}Failed: $FAILED${NC}"
    echo ""
    echo "Failed files:"
    for file in "${FAILED_FILES[@]}"; do
        echo -e "  ${RED}- $file${NC}"
    done
    exit 1
else
    echo -e "  ${RED}Failed: $FAILED${NC}"
    echo ""
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
fi

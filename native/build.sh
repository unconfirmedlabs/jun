#!/bin/bash
# Build the Zig native protobuf parser for the current platform.
# Output: native/libproto_parser.{dylib,so}
set -e
cd "$(dirname "$0")"

echo "Building native proto parser..."
zig build-lib proto_parser.zig -dynamic -OReleaseFast 2>&1

# Clean up intermediate files
rm -f proto_parser.o

echo "Built: $(ls libproto_parser.* 2>/dev/null)"

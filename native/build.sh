#!/bin/bash
# Build the Zig native checkpoint processor.
# Output: native/libcheckpoint_processor.{dylib,so}
# Requires: zig, libzstd (system)
set -e
cd "$(dirname "$0")"

echo "Building checkpoint processor..."
zig build-lib checkpoint_processor.zig -dynamic -OReleaseFast -lzstd 2>&1

rm -f checkpoint_processor.o
echo "Built: $(ls libcheckpoint_processor.* 2>/dev/null)"

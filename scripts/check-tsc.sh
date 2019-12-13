#!/bin/bash

set -ue

export OUTPUT_DIR="$(mktemp -d)"
tsc --strict --outDir ${OUTPUT_DIR}
export DIFF="$(diff -uNr lib/ ${OUTPUT_DIR})"
if [[ -n "${DIFF}" ]]; then
    echo "Diff found, failing:"
    diff -uNr lib/ ${OUTPUT_DIR}
    rm -rf ${OUTPUT_DIR}
    exit 1
fi
rm -rf ${OUTPUT_DIR}

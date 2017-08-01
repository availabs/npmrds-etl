#!/bin/bash

set -e

mkdir -p ./legacy

for f in ./sorted/*
do
  INPUT_FILE_PATH=$(realpath "$(pwd)/$f")
  OUTPUT_FILE_PATH=${INPUT_FILE_PATH//sorted/legacy}
  echo "Processing $INPUT_FILE_PATH"
  ./converter.js < "$INPUT_FILE_PATH" > "$OUTPUT_FILE_PATH" &
done

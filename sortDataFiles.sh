#!/bin/bash

set -e

mkdir -p ./sorted

# https://superuser.com/a/226489

for f in ./partitioned/*
do
  echo "Processing $f"
  INPUT_FILE_PATH=$(realpath "$(pwd)/$f")
  OUTPUT_FILE_PATH=${INPUT_FILE_PATH//partitioned/sorted}

  # Replace any '-' in tmc codes with 'n', any '+' with 'p',
  # then sort by timestamp, tmc code, vehicle type
  # outputting the result to the sorted/ directory.

  awk -F, -v OFS=, '{gsub(/-/, "n", $2); gsub(/\+/, "p", $2); print}' "$INPUT_FILE_PATH" | \
    sort -k3,3 -k2,2 -k1,1 -t',' > "$OUTPUT_FILE_PATH"

#  sort -k3,3 -k2,2 -k1,1 -t',' "$INPUT_FILE_PATH" > "$OUTPUT_FILE_PATH"

done


#!/bin/bash

set -e

mkdir -p ./partitioned/

for f in ./raw_data/*
do
 echo "Processing $f"
 # Partition the data into a file for each month.
 # We remove the header because we eventually need to sort the files.
 awk -F, 'NR>1{print >> "partitioned/"(substr($3,6,2)".csv")}' "$f"
done

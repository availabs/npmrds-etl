# Converting INRIX NPMRDS data to the HERE Schema

## Dependencies

1. Bash
2. Node.js 8.x.x
  
  NOTE: To use the latest version of Node.js, we recommend (n)[https://github.com/tj/n].

NOTE: The following scripts assume the data will be from within the same year.

## Instructions

1. `npm install`
1. `mkdir raw_data`
1. Download the zip archive(s) into the `raw_data/` directory
1. `cd raw_data`
1. `unzip <zip archive>`
1. remove all files except the data CSV(s) from the `raw_data/` directory
1. `cd ..`
1. If raw\_data CSV(s) contains data for more than a single month, run `./partitionDataByMonth.sh`.
   Else, `mkdir partitioned && mv raw_data/* partitioned/`
1. `./sortDataFiles.sh && ./convertToLegacySchema.sh` # These will take some time.


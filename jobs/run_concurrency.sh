#!/bin/bash

# --- Configuration ---
CDE_VCLUSTER_ENDPOINT="https://tcmd4r24.cde-2ppgw79d.tsel-poc.lo0mgs.c0.cloudera.site/dex/api/v1"
CDE_JOB_NAME="concurrency_poc"
MSISDN_FILE="msisdn" # MSISDN file in the same directory as this script

# --- Check if MSISDN file exists ---
if [ ! -f "$MSISDN_FILE" ]; then
    echo "Error: MSISDN file '$MSISDN_FILE' not found in the current directory."
    exit 1
fi

echo "Starting CDE job execution for '$CDE_JOB_NAME'."
echo "Reading MSISDNs from '$MSISDN_FILE' and launching jobs sequentially."

# --- Loop through each line in the MSISDN file ---
# 'read -r line' reads each line into the 'line' variable
# The 'while IFS= read -r line' construct correctly handles lines with spaces and avoids backslash interpretation
while IFS= read -r msisdn_arg; do
    # Skip empty lines
    if [ -z "$msisdn_arg" ]; then
        continue
    fi

    echo "Launching job for MSISDN: $msisdn_arg"
    
    # Execute the CDE job run command
    # Redirect all output to /dev/null to keep console clean, or remove/redirect to a log file
    ./cde job run \
        --vcluster-endpoint "$CDE_VCLUSTER_ENDPOINT" \
        --name "$CDE_JOB_NAME" \
        --arg "$msisdn_arg" > /dev/null 2>&1 &
    
    # You can add a check here for the exit status of the 'cde job run' command
    if [ $? -eq 0 ]; then
        echo "Job successfully launched for MSISDN: $msisdn_arg"
    else
        echo "Error launching job for MSISDN: $msisdn_arg"
    fi

done < "$MSISDN_FILE" # Redirects the content of MSISDN_FILE to the while loop's input

echo "All CDE jobs launched."

exit 0

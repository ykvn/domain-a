#!/bin/bash

# --- Configuration ---
CDE_VCLUSTER_ENDPOINT="https://tcmd4r24.cde-2ppgw79d.tsel-poc.lo0mgs.c0.cloudera.site/dex/api/v1"
CDE_JOB_NAME="concurrency_poc"
MSISDN_FILE="msisdn.txt" # MSISDN file in the same directory as this script

# --- Input Argument ---
# Check if a number of concurrent jobs is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <total_number_of_concurrent_jobs>"
    echo "Example: $0 10"
    exit 1
fi

MAX_PARALLEL_JOBS="$1"

# --- Check if MSISDN file exists ---
if [ ! -f "$MSISDN_FILE" ]; then
    echo "Error: MSISDN file '$MSISDN_FILE' not found in the current directory."
    exit 1
fi

echo "Starting CDE job execution for '$CDE_JOB_NAME'."
echo "Max Parallel Jobs: $MAX_PARALLEL_JOBS"

# --- Main execution using xargs for concurrency ---
# -P $MAX_PARALLEL_JOBS: Run up to MAX_PARALLEL_JOBS in parallel
# -I {}: Replace {} with each line from the input file
# --verbose: Show the commands being executed by xargs (optional, can remove if truly minimal)
# Redirect all output to /dev/null to keep console clean, or remove if you want to see it
cat "$MSISDN_FILE" | xargs -P "$MAX_PARALLEL_JOBS" -I {} bash -c '
    MSISDN_ARG="{}" # Capture the MSISDN from xargs
    JOB_NAME_VAR="'"$CDE_JOB_NAME"'" # Pass bash variables into the subshell
    ENDPOINT_VAR="'"$CDE_VCLUSTER_ENDPOINT"'"
    
    # Execute the CDE job run command
    # Output of each job run is discarded unless you remove /dev/null redirection
    ./cde job run \
        --vcluster-endpoint "$ENDPOINT_VAR" \
        --name "$JOB_NAME_VAR" \
        --arg "$MSISDN_ARG" > /dev/null 2>&1
'

echo "All CDE jobs launched. Check CDE UI for job status."

exit 0

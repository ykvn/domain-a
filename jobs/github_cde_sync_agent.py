import os
import subprocess
import time
import json
import hashlib
import sys

# --- Configuration ---
# REPO_ROOT is dynamically determined based on the script's location.
# If script is in /path/to/your/local/domain-a/jobs/github_cde_sync_agent.py
# REPO_ROOT will be /path/to/your/local/domain-a
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# The full path to the jobs directory within the cloned repository
JOBS_FULL_PATH = os.path.join(REPO_ROOT, "jobs")

# Relative path from the JOBS_FULL_PATH for the sync script
SYNC_REPO_SCRIPT_NAME = "./sync_repo"

POLLING_INTERVAL_SECONDS = 300 # Check for updates every 5 minutes (300 seconds)

# Log file location changed to /var/log/
LOG_FILE = "/var/log/cde_sync_agent.log"

# --- Logging Function ---
def log_message(message, level="INFO"):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    # Using 'try-except' block for log file writing in case of permission issues
    try:
        with open(LOG_FILE, "a") as f:
            f.write(f"[{timestamp}] [{level}] {message}\n")
    except IOError as e:
        # If logging to file fails, print to stderr to ensure message is seen
        print(f"[{timestamp}] [ERROR] Could not write to log file {LOG_FILE}: {e}", file=sys.stderr)
    print(f"[{timestamp}] [{level}] {message}") # Always print to console/stdout for immediate feedback

# --- Helper to run shell commands ---
def run_command(command, check_output=False):
    log_message(f"Executing command: {' '.join(command)}", level="DEBUG")
    try:
        if check_output:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            log_message(f"Command stdout:\n{result.stdout}", level="DEBUG")
            if result.stderr:
                log_message(f"Command stderr:\n{result.stderr}", level="WARN")
            return result.stdout.strip()
        else:
            subprocess.run(command, check=True)
            log_message("Command executed successfully.", level="DEBUG")
            return True
    except subprocess.CalledProcessError as e:
        log_message(f"Command failed with exit code {e.returncode}: {e.cmd}", level="ERROR")
        log_message(f"Stderr: {e.stderr}", level="ERROR")
        log_message(f"Stdout: {e.stdout}", level="ERROR")
        return False
    except FileNotFoundError:
        log_message(f"Command not found: {command[0]}. Make sure it's in PATH or specified with full path.", level="ERROR")
        return False

# --- Calculate MD5 hash of a file ---
def calculate_md5(filepath):
    hash_md5 = hashlib.md5()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        return None

# --- Main Logic ---
def main():
    log_message("CDE GitHub Sync Agent started.")

    # Validate REPO_ROOT existence
    if not os.path.exists(REPO_ROOT):
        log_message(f"Local repository root not found at {REPO_ROOT}. Please clone it first.", level="ERROR")
        log_message("Example: git clone https://github.com/ykvn/domain-a.git /path/to/your/local/domain-a")
        sys.exit(1)
    
    log_message(f"Changing current working directory to repository root: {REPO_ROOT}")
    os.chdir(REPO_ROOT) # Change to repo root for git operations

    # Initialize last_hashes for change detection
    last_hashes = {}
    
    # Perform initial git pull on startup to get the latest state
    log_message("Performing initial git pull on startup...")
    git_pull_success = run_command(["git", "pull"])
    if not git_pull_success:
        log_message("Initial Git pull failed. Cannot proceed.", level="CRITICAL")
        sys.exit(1)

    # Populate last_hashes AFTER the initial pull
    log_message("Performing initial scan of *.job files (after first pull).")
    if os.path.exists(JOBS_FULL_PATH):
        for root, _, files in os.walk(JOBS_FULL_PATH):
            for file in files:
                if file.endswith(".job"):
                    filepath = os.path.join(root, file)
                    last_hashes[filepath] = calculate_md5(filepath)
    log_message(f"Initial scan complete. Found {len(last_hashes)} .job files.")


    while True:
        log_message(f"Polling GitHub for updates in {REPO_ROOT}...")
        try:
            # 2. Pull latest changes from GitHub (from the repo root)
            git_pull_success = run_command(["git", "pull"])
            if not git_pull_success:
                log_message("Git pull failed. Skipping this cycle.", level="ERROR")
                time.sleep(POLLING_INTERVAL_SECONDS)
                continue

            # 3. Detect changes in *.job files
            updated_job_files = []
            current_hashes = {}
            deleted_job_files = [] # Track deleted files
            
            # Scan current state of files
            if os.path.exists(JOBS_FULL_PATH):
                for root, _, files in os.walk(JOBS_FULL_PATH):
                    for file in files:
                        if file.endswith(".job"):
                            filepath = os.path.join(root, file)
                            current_hashes[filepath] = calculate_md5(filepath)
                            
                            # Check if file is new or modified
                            if filepath not in last_hashes or last_hashes[filepath] != current_hashes[filepath]:
                                updated_job_files.append(filepath)
                                log_message(f"Detected change (new or modified) in: {filepath}")
            
            # Check for deleted files
            for old_filepath in last_hashes:
                if old_filepath not in current_hashes:
                    deleted_job_files.append(old_filepath)
                    log_message(f"Detected deletion of: {old_filepath}")

            # Update last_hashes for the next cycle
            last_hashes = current_hashes

            if updated_job_files or deleted_job_files:
                log_message(f"Detected {len(updated_job_files)} updated/new and {len(deleted_job_files)} deleted .job files. Initiating sync and job execution.")

                # *** Change directory to JOBS_FULL_PATH for executing local scripts ***
                log_message(f"Changing current working directory to jobs folder: {JOBS_FULL_PATH}")
                os.chdir(JOBS_FULL_PATH)

                # 4. Execute sync_repo (no vcluster-endpoint or name needed here as it's handled by sync_repo script)
                sync_repo_script_path = SYNC_REPO_SCRIPT_NAME
                if os.path.exists(sync_repo_script_path) and os.access(sync_repo_script_path, os.X_OK):
                    log_message(f"Executing {sync_repo_script_path}...")
                    sync_success = run_command([sync_repo_script_path])
                    if not sync_success:
                        log_message("CDE repository sync script failed. Continuing to next step but be aware.", level="WARN")
                else:
                    log_message(f"Sync repo script not found or not executable: {sync_repo_script_path}", level="ERROR")

                # 5. Execute each changed *.job file
                for job_file_path_abs in updated_job_files:
                    job_file_name = os.path.basename(job_file_path_abs) # Get just the filename from the absolute path
                    log_message(f"Executing changed job file: {job_file_name}...")
                    
                    if os.path.exists(job_file_name) and os.access(job_file_name, os.X_OK):
                        job_execute_success = run_command([job_file_name]) # Execute directly (relative to JOBS_FULL_PATH)
                        if not job_execute_success:
                            log_message(f"Execution of {job_file_name} failed.", level="ERROR")
                    else:
                        log_message(f"Job file not found or not executable in current directory: {job_file_name}", level="ERROR")
                
                # Logic for handling deleted jobs would go here if needed
                if deleted_job_files:
                    log_message("Consider adding logic to handle deletion of corresponding CDE jobs, e.g., cde job delete.", level="INFO")

                # *** Change back to REPO_ROOT for the next git pull cycle ***
                log_message(f"Changing current working directory back to repository root: {REPO_ROOT}")
                os.chdir(REPO_ROOT)

            else:
                log_message("No updates detected for *.job files.")

        except Exception as e:
            log_message(f"An unexpected error occurred: {e}", level="CRITICAL")

        log_message(f"Next poll in {POLLING_INTERVAL_SECONDS} seconds.")
        time.sleep(POLLING_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()

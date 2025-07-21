import os
import subprocess
import time
import json
import hashlib
import sys

# --- Configuration ---
REPO_PATH = "/path/to/your/local/domain-a"  # IMPORTANT: Change this to the local path of your cloned repository
JOBS_DIR_RELATIVE = "jobs"                  # Relative path to the jobs directory within the repo
SYNC_REPO_SCRIPT_RELATIVE = os.path.join(JOBS_DIR_RELATIVE, "sync_repo") # Relative path to sync_repo script
POLLING_INTERVAL_SECONDS = 300              # Check for updates every 5 minutes (300 seconds)
LOG_FILE = "/var/log/cde_sync_agent.log"    # Log file for the agent's activity

# --- CDE CLI Endpoint (from your previous commands) ---
CDE_VCLUSTER_ENDPOINT = "https://9hh897xc.cde-nvpqj9zv.se-sandb.a465-9q4k.cloudera.site/dex/api/v1"
CDE_REPO_NAME = "tsel-poc-domain-a" # Your CDE repository name

# --- Logging Function ---
def log_message(message, level="INFO"):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"[{timestamp}] [{level}] {message}\n")
    print(f"[{timestamp}] [{level}] {message}") # Also print to console for immediate feedback

# --- Helper to run shell commands ---
def run_command(command, cwd=None, check_output=False):
    log_message(f"Executing command: {' '.join(command)}", level="DEBUG")
    try:
        if check_output:
            result = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=True)
            log_message(f"Command stdout:\n{result.stdout}", level="DEBUG")
            if result.stderr:
                log_message(f"Command stderr:\n{result.stderr}", level="WARN")
            return result.stdout.strip()
        else:
            subprocess.run(command, cwd=cwd, check=True)
            log_message("Command executed successfully.", level="DEBUG")
            return True
    except subprocess.CalledProcessError as e:
        log_message(f"Command failed with exit code {e.returncode}: {e.cmd}", level="ERROR")
        log_message(f"Stderr: {e.stderr}", level="ERROR")
        log_message(f"Stdout: {e.stdout}", level="ERROR")
        return False
    except FileNotFoundError:
        log_message(f"Command not found: {command[0]}. Make sure it's in PATH.", level="ERROR")
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

    # 1. Ensure the repository is cloned
    if not os.path.exists(REPO_PATH):
        log_message(f"Local repository not found at {REPO_PATH}. Please clone it first.", level="ERROR")
        log_message("Example: git clone https://github.com/ykvn/domain-a.git /path/to/your/local/domain-a")
        sys.exit(1)

    # Dictionary to store MD5 hashes of *.job files
    last_hashes = {}

    # Initial scan to populate hashes
    log_message("Performing initial scan of *.job files.")
    full_jobs_dir = os.path.join(REPO_PATH, JOBS_DIR_RELATIVE)
    if os.path.exists(full_jobs_dir):
        for root, _, files in os.walk(full_jobs_dir):
            for file in files:
                if file.endswith(".job"):
                    filepath = os.path.join(root, file)
                    last_hashes[filepath] = calculate_md5(filepath)
    log_message(f"Initial scan complete. Found {len(last_hashes)} .job files.")

    while True:
        log_message(f"Polling GitHub for updates in {REPO_PATH}...")
        try:
            # 2. Pull latest changes from GitHub
            git_pull_success = run_command(["git", "pull"], cwd=REPO_PATH)
            if not git_pull_success:
                log_message("Git pull failed. Skipping this cycle.", level="ERROR")
                time.sleep(POLLING_INTERVAL_SECONDS)
                continue

            # 3. Detect changes in *.job files
            updated_job_files = []
            current_hashes = {}
            if os.path.exists(full_jobs_dir):
                for root, _, files in os.walk(full_jobs_dir):
                    for file in files:
                        if file.endswith(".job"):
                            filepath = os.path.join(root, file)
                            current_hashes[filepath] = calculate_md5(filepath)
                            
                            # Check if file is new or modified
                            if filepath not in last_hashes or last_hashes[filepath] != current_hashes[filepath]:
                                updated_job_files.append(filepath)
                                log_message(f"Detected change in: {filepath}")
            
            # Update last_hashes for the next cycle
            last_hashes = current_hashes

            if updated_job_files:
                log_message(f"Detected {len(updated_job_files)} updated .job files. Initiating sync and job execution.")

                # 4. Execute domain-a/jobs/sync_repo
                sync_repo_script_path = os.path.join(REPO_PATH, SYNC_REPO_SCRIPT_RELATIVE)
                if os.path.exists(sync_repo_script_path) and os.access(sync_repo_script_path, os.X_OK):
                    log_message(f"Executing {sync_repo_script_path}...")
                    sync_success = run_command([sync_repo_script_path, "--vcluster-endpoint", CDE_VCLUSTER_ENDPOINT, "--name", CDE_REPO_NAME, "-v"], cwd=REPO_PATH)
                    if not sync_success:
                        log_message("CDE repository sync script failed. Continuing to next step but be aware.", level="WARN")
                else:
                    log_message(f"Sync repo script not found or not executable: {sync_repo_script_path}", level="ERROR")

                # 5. Execute each changed *.job file
                for job_file_path in updated_job_files:
                    log_message(f"Executing changed job file: {job_file_path}...")
                    if os.path.exists(job_file_path) and os.access(job_file_path, os.X_OK):
                        # Assuming the .job file itself is executable and calls the CDE API
                        job_execute_success = run_command([job_file_path], cwd=REPO_PATH)
                        if not job_execute_success:
                            log_message(f"Execution of {job_file_path} failed.", level="ERROR")
                    else:
                        log_message(f"Job file not found or not executable: {job_file_path}", level="ERROR")
            else:
                log_message("No updates detected for *.job files.")

        except Exception as e:
            log_message(f"An unexpected error occurred: {e}", level="CRITICAL")

        log_message(f"Next poll in {POLLING_INTERVAL_SECONDS} seconds.")
        time.sleep(POLLING_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()

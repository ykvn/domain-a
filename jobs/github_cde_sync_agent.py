import os
import subprocess
import time
import json
import hashlib
import sys

# --- Configuration ---
# REPO_ROOT is dynamically determined based on the script's location.
# If this script is in /path/to/your/local/domain-a/jobs/manual_cde_deploy.py,
# then REPO_ROOT will be /path/to/your/local/domain-a.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# The full path to the jobs directory within the cloned repository
JOBS_FULL_PATH = os.path.join(REPO_ROOT, "jobs")

# Relative path from the JOBS_FULL_PATH for the sync script (e.g., "./sync_repo")
SYNC_REPO_SCRIPT_NAME = "./sync_repo"

# Log file location. Ensure the user running the script has write permissions to /var/log/
# On many systems, /var/log/ requires root permissions to write directly.
# Consider changing this to a user-writable directory like os.path.join(REPO_ROOT, "cde_deploy.log")
# or /home/your_user/cde_deploy.log if you face permission denied errors for /var/log/.
LOG_FILE = "/var/log/cde_manual_deploy.log"

# --- Logging Function ---
def log_message(message, level="INFO"):
    """Logs messages to a file and to stdout/console."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] [{level}] {message}"
    
    # Try to write to the log file. Handle permission errors gracefully.
    try:
        with open(LOG_FILE, "a") as f:
            f.write(f"{log_entry}\n")
    except IOError as e:
        # If file logging fails, print error to stderr so it's always visible
        print(f"[{timestamp}] [ERROR] Could not write to log file {LOG_FILE}: {e}", file=sys.stderr)
    
    # Always print to stdout/console
    print(log_entry)

# --- Helper to run shell commands ---
def run_command(command, cwd=None, check_output=False):
    """
    Executes a shell command.
    Args:
        command (list): A list of strings representing the command and its arguments.
        cwd (str, optional): The current working directory for the command. Defaults to None (current script's CWD).
        check_output (bool): If True, captures stdout/stderr and checks exit code.
    Returns:
        str or bool: stdout if check_output is True, otherwise True for success, False for failure.
    """
    log_message(f"Executing command: {' '.join(command)}", level="DEBUG")
    try:
        if check_output:
            result = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=True)
            if result.stdout:
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
        log_message(f"Command not found: {command[0]}. Make sure it's in PATH or specified with full path.", level="ERROR")
        return False

# --- Calculate MD5 hash of a file ---
def calculate_md5(filepath):
    """Calculates the MD5 hash of a given file."""
    hash_md5 = hashlib.md5()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        log_message(f"File not found for hash calculation: {filepath}", level="DEBUG")
        return None
    except Exception as e:
        log_message(f"Error calculating hash for {filepath}: {e}", level="ERROR")
        return None

# --- Main Logic ---
def main():
    log_message("CDE Manual Deployment Agent started.")

    # 1. Validate REPO_ROOT existence
    if not os.path.exists(REPO_ROOT):
        log_message(f"Local repository root not found at {REPO_ROOT}. Please clone it first.", level="ERROR")
        log_message("Example: git clone https://github.com/ykvn/domain-a.git /path/to/your/local/domain-a")
        sys.exit(1)
    
    log_message(f"Changing current working directory to repository root: {REPO_ROOT}")
    os.chdir(REPO_ROOT) # Change to repo root for git operations

    # Store hashes *before* the pull to detect what changed in this run
    hashes_before_pull = {}
    if os.path.exists(JOBS_FULL_PATH):
        for root, _, files in os.walk(JOBS_FULL_PATH):
            for file in files:
                if file.endswith(".job"):
                    filepath = os.path.join(root, file)
                    hashes_before_pull[filepath] = calculate_md5(filepath)
    log_message(f"Scan before pull complete. Found {len(hashes_before_pull)} .job files.")

    # 2. Pull latest changes from GitHub
    log_message("Performing git pull...")
    git_pull_success = run_command(["git", "pull"], cwd=REPO_ROOT) # Explicitly pass cwd for git pull
    if not git_pull_success:
        log_message("Git pull failed. Aborting deployment.", level="CRITICAL")
        sys.exit(1)

    # 3. Detect changes in *.job files after the pull
    updated_job_files = []
    current_hashes = {}
    deleted_job_files = [] # Optional: Track deleted files for logging

    if os.path.exists(JOBS_FULL_PATH):
        for root, _, files in os.walk(JOBS_FULL_PATH):
            for file in files:
                if file.endswith(".job"):
                    filepath = os.path.join(root, file)
                    current_hashes[filepath] = calculate_md5(filepath)
                    
                    # Check if file is new or modified compared to before the pull
                    if filepath not in hashes_before_pull or hashes_before_pull[filepath] != current_hashes[filepath]:
                        updated_job_files.append(filepath)
                        log_message(f"Detected change (new or modified) in: {filepath}")
        
        # Check for deleted files
        for old_filepath in hashes_before_pull:
            if old_filepath not in current_hashes:
                deleted_job_files.append(old_filepath)
                log_message(f"Detected deletion of: {old_filepath}")

    if updated_job_files or deleted_job_files:
        log_message(f"Detected {len(updated_job_files)} updated/new and {len(deleted_job_files)} deleted .job files. Initiating sync and job execution.")

        # Change directory to JOBS_FULL_PATH for executing local scripts
        log_message(f"Changing current working directory to jobs folder: {JOBS_FULL_PATH}")
        os.chdir(JOBS_FULL_PATH)

        # 4. Execute sync_repo
        sync_repo_script_name_only = os.path.basename(SYNC_REPO_SCRIPT_NAME) # Get just the filename
        sync_repo_script_abs_path = os.path.join(JOBS_FULL_PATH, sync_repo_script_name_only)

        if os.path.exists(sync_repo_script_abs_path):
            log_message(f"Setting executable permission for {sync_repo_script_name_only}...", level="DEBUG")
            os.chmod(sync_repo_script_abs_path, 0o755) # rwxr-xr-x permissions
        
        # Execute using './' prefix as it's now in the current directory
        if os.path.exists(sync_repo_script_name_only) and os.access(sync_repo_script_name_only, os.X_OK):
            log_message(f"Executing {sync_repo_script_name_only}...", level="INFO")
            sync_success = run_command([f"./{sync_repo_script_name_only}"]) # Use './' prefix
            if not sync_success:
                log_message("CDE repository sync script failed. Continuing to next step but be aware.", level="WARN")
        else:
            log_message(f"Sync repo script not found or not executable: {sync_repo_script_name_only}", level="ERROR")

        # 5. Execute each changed *.job file
        for job_file_path_abs in updated_job_files:
            job_file_name = os.path.basename(job_file_path_abs) # Get just the filename from the absolute path

            # Ensure the job file is executable before trying to run it
            if os.path.exists(job_file_path_abs): # Check existence using absolute path
                log_message(f"Setting executable permission for {job_file_name}...", level="DEBUG")
                os.chmod(job_file_path_abs, 0o755) # rwxr-xr-x permissions
            
            # Now try to execute using './' prefix, as we are in JOBS_FULL_PATH
            if os.path.exists(job_file_name) and os.access(job_file_name, os.X_OK):
                log_message(f"Executing changed job file: {job_file_name}...", level="INFO")
                job_execute_success = run_command([f"./{job_file_name}"]) # Use './' prefix
                if not job_execute_success:
                    log_message(f"Execution of {job_file_name} failed.", level="ERROR")
            else:
                log_message(f"Job file not found or not executable in current directory after setting permissions: {job_file_name}", level="ERROR")
        
        if deleted_job_files:
            log_message("INFO: Deleted .job files were detected. Remember to manually delete corresponding CDE jobs if necessary.", level="INFO")

    else:
        log_message("No updates detected for *.job files after git pull. Nothing to deploy.")

    log_message("CDE Manual Deployment Agent finished.")

if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
"""
Core logic for the PyBackup sequential multi-target backup tool.

Handles directory traversal, file comparison, copying, target switching,
and communication with the GUI thread. Uses a recursive synchronization
approach without a full pre-scan.
"""

import os
import shutil
import time
import logging
import sys
import queue  # Used by GUI, not directly here now
import threading  # Used by GUI
import json  # Import needed for load/save state
from typing import Optional, Tuple, List, Dict, Any

# --- Constants ---

# Directories to ignore during target clearing (case-insensitive).
IGNORE_DIRS_ON_CLEAR = {
    # Windows
    'System Volume Information',
    '$RECYCLE.BIN',
    # macOS
    '.DS_Store',
    '.localized',
    '.fseventsd',
    '.Spotlight-V100',
    '.Trashes',
    # Linux
    '.gvfs',
    'lost+found'
}
IGNORE_DIRS_ON_CLEAR_LOWER = {d.lower() for d in IGNORE_DIRS_ON_CLEAR}

# Default values if not found in config dict (used internally as fallback)
INTERNAL_DEFAULT_FREE_PERCENT = 10
INTERNAL_DEFAULT_RETRIES = 3
INTERNAL_DEFAULT_DELAY = 10


# --- Backup Engine Class ---

class BackupEngine:
    """
    Manages the backup process, including state, file operations, and
    communication with the controlling thread (e.g., GUI).
    """

    def __init__(self, source_dir: str, target_dirs: List[str],
                 config: Dict[str, Any], progress_queue: queue.Queue,
                 log_queue: queue.Queue, pause_event: threading.Event,
                 cancel_event: threading.Event, state_file: str):
        """
        Initialize the BackupEngine.

        Args:
            source_dir: Absolute path to the source directory.
            target_dirs: List of absolute paths to target directories.
            config: Dictionary containing configuration like 'free_percent',
                    'retries', 'delay'. Values MUST be provided by caller.
            progress_queue: Queue to send progress/status updates to the GUI.
            log_queue: Queue to send log messages to the GUI.
            pause_event: Threading event to signal pause request.
            cancel_event: Threading event to signal cancellation request.
            state_file: Path to the JSON file for saving/loading resume state.
        """
        self.source_dir: str = source_dir
        self.target_dirs: List[str] = target_dirs
        self.config: Dict[str, Any] = config
        self.config.setdefault('free_percent', INTERNAL_DEFAULT_FREE_PERCENT)
        self.config.setdefault('retries', INTERNAL_DEFAULT_RETRIES)
        self.config.setdefault('delay', INTERNAL_DEFAULT_DELAY)

        self.progress_queue: queue.Queue = progress_queue
        self.log_queue: queue.Queue = log_queue
        self.pause_event: threading.Event = pause_event
        self.cancel_event: threading.Event = cancel_event
        self.state_file: str = state_file

        # Runtime state initialized by _load_resume_state
        self.target_index: int = 0
        self.current_target_base: Optional[str] = None
        self.targets_initialized_this_run: List[str] = []
        self.last_processed_path: Optional[str] = None
        self.is_resuming: bool = False

        # Statistics for the current run (reset each time)
        self.total_items_processed_this_run: int = 0
        self.total_size_copied_this_run: int = 0
        self.items_processed_this_target: int = 0
        self.size_copied_this_target: int = 0
        self.last_item_logged_this_target: str = "N/A"  # For target full message
        self.failed_items: List[Tuple[str, str]] = []

    def _emit_log(self, level: int, message: str, **kwargs):
        """Safely put a log message onto the log queue."""
        try:
            log_record = {'level': level, 'message': message}
            log_record.update(kwargs)
            self.log_queue.put(log_record)
        except (queue.Full, TypeError) as e:
            # Avoid crashing the core logic if GUI queue fails
            print(f"ERROR: Failed to queue log message: {e}", file=sys.stderr)

    def _emit_progress(self, msg_type: str, **kwargs):
        """Safely put a progress update onto the progress queue."""
        try:
            payload = {'type': msg_type}
            payload.update(kwargs)
            self.progress_queue.put(payload)
        except (queue.Full, TypeError) as e:
            print(
                f"ERROR: Failed to queue progress message: {e}", file=sys.stderr)

    def _human_readable_size(self, size_bytes: float) -> str:
        """Convert bytes to a human-readable string."""
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = 0
        while size_bytes >= 1024 and i < len(size_name) - 1:
            size_bytes /= 1024.0
            i += 1
        return f"{size_bytes:.2f} {size_name[i]}"

    def _get_disk_usage(self, path: str) -> Optional[Any]:
        """Get disk usage for the filesystem containing the specified path."""
        abs_path = os.path.abspath(path)
        # Drive letter or mount point root
        drive_path = os.path.splitdrive(abs_path)[0] + os.sep
        try:
            # Attempt getting usage from the drive root first
            return shutil.disk_usage(drive_path)
        except OSError:
            # Fallback for network paths or systems where drive root fails
            try:
                logging.debug(
                    "Retrying disk usage check directly on: %s", abs_path)
                return shutil.disk_usage(abs_path)
            except OSError as e:
                # Log if both attempts fail
                self._emit_log(
                    logging.ERROR,
                    f"Could not determine disk usage for path '{abs_path}'. Error: {e}"
                )
                return None

    def _get_free_space_margin(self, path: str) -> Tuple[float, float]:
        """Calculate required free space margin and current free space."""
        usage = self._get_disk_usage(path)
        if usage:
            # Read percentage from config stored in the instance
            free_perc = self.config['free_percent']
            required_margin = usage.total * (free_perc / 100.0)
            return required_margin, usage.free
        else:
            # Cannot determine usage, skip check but log warning
            self._emit_log(
                logging.WARNING,
                f"Cannot determine disk usage for '{path}'. Free space check skipped."
            )
            return 0, 0  # Assume check passes if usage unknown

    def _clear_target_directory(self, target_path: str) -> bool:
        """Remove contents of target dir, skipping predefined folders."""
        self._emit_log(
            logging.INFO, f"Clearing contents of target directory: {target_path}")
        try:
            os.makedirs(target_path, exist_ok=True)
        except OSError as e:
            self._emit_log(
                logging.ERROR, f"Failed create/access target '{target_path}': {e}. Stop clear.")
            return False

        items_cleared_or_skipped = 0
        items_failed_to_clear = 0
        try:
            # Iterate through items directly inside the target path
            for item_name in os.listdir(target_path):
                if self.cancel_event.is_set():
                    return False  # Check cancellation
                item_path = os.path.join(target_path, item_name)

                # Skip ignored system/hidden directories
                if item_name.lower() in IGNORE_DIRS_ON_CLEAR_LOWER:
                    self._emit_log(
                        logging.INFO, f"Skipping ignored item during clear: {item_path}")
                    items_cleared_or_skipped += 1
                    continue

                # Attempt to remove the item based on its type
                try:
                    if os.path.islink(item_path) or os.path.isfile(item_path):
                        logging.debug("Removing during clear: %s", item_path)
                        os.remove(item_path)
                        items_cleared_or_skipped += 1
                    elif os.path.isdir(item_path):
                        logging.debug(
                            "Removing tree during clear: %s", item_path)
                        shutil.rmtree(item_path)
                        items_cleared_or_skipped += 1
                    else:
                        logging.warning(
                            "Skipping unknown item type during clear: %s", item_path)
                        items_cleared_or_skipped += 1
                except OSError as e:  # Catch errors removing individual items
                    self._emit_log(
                        logging.WARNING, f"Failed remove item during clear {item_path}: {e}. Skip.")
                    items_failed_to_clear += 1

            # Log summary of the clear operation for this directory
            if items_failed_to_clear > 0:
                self._emit_log(
                    logging.WARNING,
                    f"Finished clear attempt: {target_path}. "
                    f"{items_failed_to_clear} items failed removal."
                )
            else:
                self._emit_log(
                    logging.INFO,
                    f"Finished clear attempt: {target_path}. "
                    f"{items_cleared_or_skipped} items cleared or skipped."
                )
            return True  # Clearing attempt finished

        except OSError as e:  # Catch errors listing the directory itself
            self._emit_log(
                logging.ERROR, f"Failed list/clear target '{target_path}': {e}.")
            return False

    def _copy_file_with_retry(self, src_file: str, dest_file: str) -> bool:
        """Copy a single file with metadata preservation and retry logic."""
        retries = self.config['retries']
        delay = self.config['delay']

        for attempt in range(retries):
            # Check for pause/cancel before each attempt
            if self.cancel_event.is_set():
                return False
            while self.pause_event.is_set():
                time.sleep(0.5)  # Use a short sleep

            try:
                dest_dir = os.path.dirname(dest_file)
                # Ensure directory exists using simpler, robust method (handles race conditions)
                os.makedirs(dest_dir, exist_ok=True)

                # Emit status just before the potentially blocking copy operation
                try:
                    file_size_hr = self._human_readable_size(
                        os.path.getsize(src_file))
                    status_msg = f"Copying ({file_size_hr})..."
                except OSError:  # Ignore error getting size for status update
                    status_msg = "Copying..."
                self._emit_progress(
                    'status', item=src_file, message=status_msg,
                    destination_path=dest_file
                )

                # Copy file and metadata, do not follow source link
                shutil.copy2(src_file, dest_file, follow_symlinks=False)
                return True  # Success

            except OSError as e:
                # Handle common OS errors like permissions or disk full
                self._emit_log(
                    logging.WARNING,
                    f"Attempt {attempt+1}/{retries} OS error during copy/makedirs: {e}"
                )

            # If attempt failed and more retries are allowed
            if attempt < retries - 1:
                # Check again for pause/cancel before sleeping
                if self.cancel_event.is_set():
                    return False
                while self.pause_event.is_set():
                    time.sleep(0.5)
                if self.cancel_event.is_set():
                    return False

                self._emit_log(
                    logging.INFO, f"Retrying copy in {delay} seconds...")
                time.sleep(delay)
            else:
                # Log final failure after all retries
                self._emit_log(
                    logging.ERROR, f"Failed copy after {retries} attempts: {src_file}")
                return False  # Indicate failure

        return False  # Should not be reached normally

    def _load_resume_state(self):
        """Load resume state (last processed item, target index) for the engine."""
        state_file = self.state_file
        # Initialize with default values first
        default_values = {"last_processed": None, "target_index_for_last": 0}
        state = default_values

        if not os.path.exists(state_file):
            self._emit_log(
                logging.INFO, "No resume state file found. Starting fresh.")
            # state remains default_values
        else:
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                # Validate keys and types more strictly after loading
                if (isinstance(loaded_data, dict) and
                        "last_processed" in loaded_data and
                        "target_index_for_last" in loaded_data and
                        (loaded_data["last_processed"] is None or
                         isinstance(loaded_data["last_processed"], str)) and
                        isinstance(loaded_data["target_index_for_last"], int)):
                    state = loaded_data  # Use loaded data only if valid
                    self._emit_log(
                        logging.INFO, f"Loaded resume state from {state_file}")
                else:
                    self._emit_log(
                        logging.WARNING,
                        f"State file {state_file} invalid format/keys. Start fresh."
                    )
                    # state remains default_values
            # Catch JSON errors and explicit validation errors
            except ValueError as e:  # Catches JSONDecodeError and type errors
                self._emit_log(
                    logging.ERROR,
                    f"Error reading state file {state_file}: {e}. Start fresh."
                )
                # state remains default_values
            except OSError as e:
                self._emit_log(
                    logging.ERROR,
                    f"Error loading state file {state_file}: {e}. Start fresh."
                )
                # state remains default_values

        # Set engine attributes based on final state (loaded or default)
        self.last_processed_path = state["last_processed"]
        self.target_index = state["target_index_for_last"]
        self.is_resuming = self.last_processed_path is not None

    def _save_resume_state(self):
        """Save the current resume state (last item, target index) atomically."""
        state_data = {
            "last_processed": self.last_processed_path,
            "target_index_for_last": self.target_index
        }
        # Write to a temporary file first, then replace original for atomicity
        temp_state_file = self.state_file + '.tmp'
        try:
            with open(temp_state_file, 'w', encoding='utf-8') as f:
                # Use indent for readability
                json.dump(state_data, f, indent=4)
            # os.replace is atomic on most platforms
            os.replace(temp_state_file, self.state_file)
            logging.debug(
                "Saved state: Last='%s' on idx %d",
                self.last_processed_path, self.target_index
            )
        except Exception as e:  # pylint: disable=broad-except
            # Log critical error if state cannot be saved, as resume will fail
            self._emit_log(
                logging.ERROR, f"CRITICAL: Failed to save resume state: {e}")

    def _ensure_target_initialized(self) -> bool:
        """Ensure the current target directory exists and is cleared once per run."""
        if self.current_target_base is None:
            # This should not happen if load/switch logic is correct
            self._emit_log(
                logging.ERROR, "Current target base is not set. Cannot initialize.")
            return False
        # Check if we already prepared this target during *this* execution
        if self.current_target_base not in self.targets_initialized_this_run:
            self._emit_log(
                logging.INFO, f"Preparing target for this run: {self.current_target_base}")
            # Attempt to clear contents (selectively)
            if not self._clear_target_directory(self.current_target_base):
                # Error already logged by clear function
                return False  # Indicate failure to prepare
            # Mark as initialized for this run and reset local stats
            self.targets_initialized_this_run.append(self.current_target_base)
            self.items_processed_this_target = 0
            self.size_copied_this_target = 0
            self.last_item_logged_this_target = "N/A"
            self._emit_log(
                logging.INFO, f"Target '{self.current_target_base}' prepared.")
        return True  # Indicate success or already initialized this run

    def _switch_target(self, current_item_path: str, item_size: int = 0) -> str:
        """Switch to the next target disk, prepare it, and return new dest path.

        Raises:
            RuntimeError: If out of target disks or the new target cannot be
                          prepared or doesn't have enough space.
        """
        self._emit_log(
            logging.INFO,
            f"Target '{self.current_target_base}' appears full "
            f"(checking for {self._human_readable_size(item_size)})."
        )
        # Emit stats about the target that just filled up
        self._emit_progress(
            'target_full_stats',
            target=self.current_target_base,
            items=self.items_processed_this_target,
            size=self.size_copied_this_target,
            last_item=self.last_item_logged_this_target
        )

        # Save state *before* changing the target index
        self._save_resume_state()
        self.target_index += 1

        # Check if we ran out of target disks
        if self.target_index >= len(self.target_dirs):
            msg = f"Ran out of targets. Cannot process: {current_item_path}"
            self._emit_log(logging.ERROR, msg)
            self.failed_items.append(
                (current_item_path, "Ran out of target space"))
            raise RuntimeError("Ran out of target space")

        # Update engine state for the new target
        self.current_target_base = self.target_dirs[self.target_index]
        self.items_processed_this_target = 0
        self.size_copied_this_target = 0
        self.last_item_logged_this_target = "N/A"
        self._emit_log(
            logging.INFO, f"Switching to next target: {self.current_target_base}")
        self._emit_progress(
            'target_switch', index=self.target_index, path=self.current_target_base
        )

        # Ensure the new target directory is prepared (cleared once per run)
        if not self._ensure_target_initialized():
            msg = f"Failed prepare new target '{self.current_target_base}'"
            self._emit_log(logging.ERROR, msg)
            raise RuntimeError(msg)

        # Re-check space on the new drive immediately for the current item
        required_margin, current_free = self._get_free_space_margin(
            self.current_target_base)
        if required_margin > 0 and current_free < item_size + required_margin:
            msg = (
                f"Insufficient space on NEW target '{self.current_target_base}' "
                f"for {current_item_path} "
                f"(Size: {self._human_readable_size(item_size)})."
            )
            self._emit_log(logging.ERROR, msg)
            self.failed_items.append(
                (current_item_path, "Insufficient space on new target"))
            raise RuntimeError("Insufficient space on new target")

        # Calculate and return the new destination path for the item being processed
        try:
            relative_path = os.path.relpath(current_item_path, self.source_dir)
            return os.path.join(self.current_target_base, relative_path)
        except ValueError as e:
            # This should ideally not happen if source_path is always valid
            msg = f"Path calculation error after switch for {current_item_path}: {e}"
            self._emit_log(logging.ERROR, msg)
            self.failed_items.append(
                (current_item_path, "Path calculation error after switch")
            )
            raise RuntimeError(msg) from e

    def _process_directory_recursive(self, current_source_dir: str):
        """
        Recursively process a directory: clean destination, sync source items.

        This is the core recursive function. It lists source and destination,
        removes extra items from destination, then iterates through sorted source
        items, handling subdirectories via recursion and files/links via copy/sync
        logic, including target switching if space runs out.

        Args:
            current_source_dir: The absolute path of the source directory currently
                                being processed.
        """
        # Check for cancellation at the start of processing a directory
        if self.cancel_event.is_set():
            return

        # Determine corresponding destination directory path based on current target
        try:
            relative_dir_path = os.path.relpath(
                current_source_dir, self.source_dir)
            # Handle root case where relpath is '.' to avoid joining issues
            current_dest_dir = self.current_target_base if relative_dir_path == '.' \
                else os.path.join(self.current_target_base, relative_dir_path)
        except ValueError as e:
            # This can happen if current_source_dir somehow isn't under source_dir
            self._emit_log(
                logging.ERROR,
                f"Path calculation error for dir {current_source_dir}: {e}. Skip recursion."
            )
            self.failed_items.append((current_source_dir, f"Path error: {e}"))
            return

        # Emit status update indicating scanning/comparing this directory
        self._emit_progress(
            'status', current_dir=current_source_dir, message="Scanning/Comparing...",
            destination_path=current_dest_dir
        )

        # --- List source and destination directories safely ---
        try:
            source_items_list = os.listdir(current_source_dir)
            source_items_set = set(source_items_list)
        except OSError as e:
            # Log error and stop processing this directory if source is unreadable
            self._emit_log(
                logging.ERROR, f"Cannot list source dir {current_source_dir}: {e}")
            self.failed_items.append(
                (current_source_dir, f"Cannot list source: {e}"))
            return

        try:
            # Ensure destination directory exists before listing its contents
            os.makedirs(current_dest_dir, exist_ok=True)
            dest_items_set = set(os.listdir(current_dest_dir))
        except (OSError, ValueError, RuntimeError) as e:
            # If destination cannot be listed/created, log warning and skip cleanup
            self._emit_log(
                logging.WARNING,
                f"Cannot list/create dest dir {current_dest_dir}: {e}. Cleanup skipped."
            )
            dest_items_set = set()  # Assume empty destination if listing fails

        # --- Cleanup Destination: Remove extra items not in source ---
        items_to_delete = dest_items_set - source_items_set
        # Filter out system/hidden directories that should not be deleted
        items_to_delete = {item for item in items_to_delete
                           if item.lower() not in IGNORE_DIRS_ON_CLEAR_LOWER}

        for item_name in items_to_delete:
            # Check cancellation frequently during potentially long cleanup
            if self.cancel_event.is_set():
                return
            # Allow pausing during cleanup phase as well
            while self.pause_event.is_set():
                time.sleep(0.5)

            item_path_dest = os.path.join(current_dest_dir, item_name)
            self._emit_progress(
                'status', current_dir=current_source_dir, item=item_path_dest,
                message="Deleting...", destination_path=item_path_dest
            )
            try:
                # Remove item based on type
                if os.path.islink(item_path_dest) or os.path.isfile(item_path_dest):
                    os.remove(item_path_dest)
                elif os.path.isdir(item_path_dest):
                    # Use shutil.rmtree for directories
                    shutil.rmtree(item_path_dest)
                self._emit_log(
                    logging.INFO, f"Deleted extra item: {item_path_dest}")
            except (OSError, ValueError) as e:
                # Log failure but continue cleanup for other items
                self._emit_log(logging.WARNING,
                               f"Failed delete: {item_path_dest} ({e})")
                self.failed_items.append(
                    (item_path_dest, f"Failed delete: {e}"))

        # --- Process Source Items (Sorted within this directory) ---
        source_items_list.sort()  # Ensure consistent order within directory
        items_in_dir = len(source_items_list)

        for i, item_name in enumerate(source_items_list):
            # Check cancellation and pause frequently
            if self.cancel_event.is_set():
                return
            while self.pause_event.is_set():
                time.sleep(0.5)

            source_path = os.path.join(current_source_dir, item_name)

            # --- Resume Check: Skip items already processed in previous runs ---
            if self.is_resuming and self.last_processed_path and source_path <= self.last_processed_path:  # pylint: disable=line-too-long
                logging.debug("Skipping (resume check): %s <= %s",
                              source_path, self.last_processed_path)
                continue
            # Stop initial resume skipping mode after passing the resume point
            if self.is_resuming:
                self._emit_log(
                    logging.INFO, f"Resume point reached. Processing from: {source_path}")
                self.is_resuming = False  # Now process items normally

            # Calculate destination path based on *current* target base
            # Must recalculate here as target might have switched while processing previous file
            try:
                relative_path = os.path.relpath(source_path, self.source_dir)
                destination_path = os.path.join(
                    self.current_target_base, relative_path)
            except ValueError as e:
                self._emit_log(
                    logging.ERROR, f"Path error {source_path}: {e}. Skip.")
                self.failed_items.append((source_path, f"Path error: {e}"))
                continue

            # Signal start of processing this item
            self._emit_progress(
                'item_start', source_path=source_path,
                destination_path=destination_path, item_index=i,
                total_items_in_dir=items_in_dir
            )

            # Determine item type safely
            is_link = os.path.islink(source_path)
            # isdir/isfile checks should exclude links to avoid double processing
            is_dir = os.path.isdir(source_path) and not is_link
            is_file = os.path.isfile(source_path) and not is_link
            operation_successful = False  # Track success for state saving

            # --- Process Based on Type ---
            if is_dir:
                self._emit_progress(
                    'status', current_dir=current_source_dir, item=source_path,
                    message="Entering dir...", destination_path=destination_path
                )
                try:
                    # Ensure destination directory exists before recursing
                    os.makedirs(destination_path, exist_ok=True)
                    # Recursively process the subdirectory
                    self._process_directory_recursive(source_path)
                    # Note: Directory structure creation itself doesn't update state/counts
                except (OSError, ValueError) as e:
                    # Log errors creating or processing subdirectory
                    self._emit_log(
                        logging.ERROR, f"Cannot create/process dir {destination_path}: {e}")
                    self.failed_items.append((source_path, f"Dir fail: {e}"))

            elif is_link:
                self._emit_progress(
                    'status', current_dir=current_source_dir, item=source_path,
                    message="Processing link...", destination_path=destination_path
                )
                process_link = True
                # Check if destination link already exists (simple check)
                if os.path.islink(destination_path):
                    try:
                        # Could add readlink comparison here for more robustness if needed
                        self._emit_log(
                            logging.INFO, f"Dest link exists, skip create: {destination_path}")
                        process_link = False
                    except (OSError, ValueError) as e:
                        self._emit_log(
                            logging.WARNING, f"Error check dest link {destination_path}: {e}. Will try create.")  # pylint: disable=line-too-long

                if process_link:
                    try:
                        link_target = os.readlink(source_path)
                        # Check if link target is a directory to pass hint to os.symlink
                        target_is_dir_hint = os.path.isdir(source_path)
                        # Ensure parent directory exists
                        os.makedirs(os.path.dirname(
                            destination_path), exist_ok=True)
                        # Remove existing non-link item at destination if necessary
                        if os.path.exists(destination_path) and not os.path.islink(destination_path):  # pylint: disable=line-too-long
                            self._emit_log(
                                logging.WARNING, f"Removing non-link at {destination_path} pre-link.")  # pylint: disable=line-too-long
                            if os.path.isfile(destination_path):
                                os.remove(destination_path)
                            elif os.path.isdir(destination_path):
                                shutil.rmtree(destination_path)
                        # Create the symbolic link
                        os.symlink(link_target, destination_path,
                                   target_is_directory=target_is_dir_hint)
                        self._emit_log(
                            logging.INFO, f"Created symlink: {destination_path} -> {link_target}")
                        operation_successful = True  # Mark for state save
                    except (OSError, ValueError) as e:
                        # Catch OSError (permissions, FS support) and other errors
                        self._emit_log(logging.WARNING,
                                       f"Symlink fail: {e}. Skip.")
                        self.failed_items.append(
                            (source_path, f"Symlink fail: {e}"))

            elif is_file:
                # Initial status update before size check
                self._emit_progress(
                    'status', current_dir=current_source_dir, item=source_path,
                    message="Processing file...", destination_path=destination_path
                )
                process_file = True
                try:
                    file_size = os.path.getsize(source_path)
                except OSError as e:
                    self._emit_log(
                        logging.ERROR, f"Size error: {source_path}: {e}")
                    self.failed_items.append((source_path, f"Size error: {e}"))
                    continue  # Skip this file

                # Check if destination exists and matches source (size/mtime)
                if os.path.exists(destination_path) and not os.path.islink(destination_path):
                    try:
                        dest_stat = os.stat(destination_path)
                        source_mtime = int(os.path.getmtime(source_path))
                        # Skip if size matches and source is not newer
                        if file_size == dest_stat.st_size and source_mtime <= int(dest_stat.st_mtime):  # pylint: disable=line-too-long
                            self._emit_log(
                                logging.INFO, f"Skip matching file: {destination_path}")
                            process_file = False
                        else:
                            self._emit_log(
                                logging.WARNING, f"Dest differs: {destination_path}. Overwrite.")
                    except (OSError, ValueError) as e:
                        # If metadata comparison fails, assume copy is needed
                        self._emit_log(logging.WARNING,
                                       f"Meta compare error: {e}. Will copy.")

                if process_file:
                    # Check Disk Space before attempting copy
                    required_margin, current_free = self._get_free_space_margin(
                        self.current_target_base)
                    # If file doesn't fit (considering margin), switch target
                    if required_margin > 0 and current_free < file_size + required_margin:
                        try:
                            # Switch target, get updated destination path for this file
                            destination_path = self._switch_target(
                                source_path, file_size)
                            # Flag that first item logging might be needed on new target
                            # (Handled within _copy_file_with_retry's status emit now)
                        except RuntimeError:
                            # Fatal error during switch (e.g., out of all disks)
                            # Exception logged by _switch_target, just stop processing this dir
                            return

                    # Copy the file using the (potentially updated) destination path
                    if self._copy_file_with_retry(source_path, destination_path):
                        # Update statistics on successful copy
                        self.items_processed_this_target += 1
                        self.size_copied_this_target += file_size
                        self.total_size_copied_this_run += file_size
                        self.last_item_logged_this_target = source_path
                        operation_successful = True  # Mark for state save
                    else:
                        # Add to failed items if copy failed after retries
                        self.failed_items.append((source_path, "Copy failed"))

            else:  # Item is not a dir, link, or file
                self._emit_log(logging.warning,
                               f"Skip unknown type: {source_path}")
                self.failed_items.append((source_path, "Unknown type"))

            # --- Post-Processing for this Item ---
            # Update overall processed count and save state ONLY on success
            if operation_successful:
                self.total_items_processed_this_run += 1
                self.last_processed_path = source_path  # Update last successful path
                self._save_resume_state()  # Save state includes current target_index
                # Send cumulative stats for this run to GUI
                self._emit_progress(
                    'progress_update',
                    items_processed=self.total_items_processed_this_run,
                    size_copied=self.total_size_copied_this_run
                )

            # Signal item completion regardless of success for UI update
            self._emit_progress(
                'item_done', source_path=source_path,
                destination_path=destination_path, success=operation_successful
            )

        # --- Finished processing items in current_source_dir ---
        self._emit_progress(
            'status', current_dir=current_source_dir, message="Directory done.",
            destination_path=current_dest_dir
        )

    def run_backup(self):
        """Main entry point to start the backup process for this engine."""
        self._emit_log(logging.INFO, "Backup engine started.")
        try:
            # Load initial state which sets target_index, last_processed_path
            self._load_resume_state()

            # Validate target index right after load
            if not self.target_dirs:
                # Should be caught by GUI validation too
                raise RuntimeError("No target directories specified.")
            if not 0 <= self.target_index < len(self.target_dirs):
                # If index from state file is invalid, reset to 0 and clear state
                self._emit_log(
                    logging.WARNING,
                    f"Invalid target index {self.target_index} from state. Resetting to 0."
                )
                self.target_index = 0
                self.last_processed_path = None  # Force re-check from start
                self.is_resuming = False
                self._save_resume_state()  # Save the reset state

            # Set initial target base based on loaded/validated index
            self.current_target_base = self.target_dirs[self.target_index]

            # Prepare the initial target directory (clear if needed)
            if not self._ensure_target_initialized():
                raise RuntimeError(
                    "Failed to initialize first target directory.")

            # Start the recursive processing from the root source directory
            self._process_directory_recursive(self.source_dir)

            # Check cancellation flag after recursion naturally finishes or is interrupted
            if self.cancel_event.is_set():
                self._emit_log(logging.WARNING, "Backup process cancelled.")
                self._emit_progress(
                    'cancelled',
                    total_items=self.total_items_processed_this_run,
                    total_size=self.total_size_copied_this_run,
                    failed_items=self.failed_items
                )
            else:
                # Signal normal completion if not cancelled
                self._emit_log(
                    logging.INFO, "Backup process finished normally.")
                # Optionally reset state file on successful full completion?
                # self._save_resume_state(None, 0) # Reset state
                self._emit_progress(
                    'done',
                    total_items=self.total_items_processed_this_run,
                    total_size=self.total_size_copied_this_run,
                    failed_items=self.failed_items
                )

        except RuntimeError as e:
            # Handle fatal errors raised explicitly (out of space, init fail, etc.)
            self._emit_log(logging.ERROR, f"Backup aborted: {e}")
            self._emit_progress('error', message=str(
                e), failed_items=self.failed_items)
        except Exception as e:  # pylint: disable=broad-except
            # Handle unexpected errors during the process
            self._emit_log(
                logging.ERROR, f"Unexpected error during backup: {e}", exc_info=True)
            self._emit_progress(
                'error', message=f"Unexpected error: {e}", failed_items=self.failed_items)
        finally:
            # Always signal engine stop
            self._emit_log(logging.INFO, "Backup engine stopped.")


# --- Wrapper Function for Threading ---

def start_backup_session(source_dir: str, target_dirs: List[str],
                         config: Dict[str, Any], state_file: str,
                         progress_queue: queue.Queue, log_queue: queue.Queue,
                         pause_event: threading.Event, cancel_event: threading.Event):
    """
    Wrapper function to create and run the BackupEngine instance.
    Intended to be the target of the worker thread created by the GUI.

    Args:
        source_dir: Source directory path.
        target_dirs: List of target directory paths.
        config: Dictionary with configuration options ('free_percent', etc.).
        state_file: Path to the resume state file.
        progress_queue: Queue for progress updates to the GUI.
        log_queue: Queue for log messages to the GUI.
        pause_event: Event for pausing the process.
        cancel_event: Event for cancelling the process.
    """
    engine = BackupEngine(
        source_dir=source_dir, target_dirs=target_dirs, config=config,
        progress_queue=progress_queue, log_queue=log_queue,
        pause_event=pause_event, cancel_event=cancel_event,
        state_file=state_file
    )
    engine.run_backup()

# -*- coding: utf-8 -*-
# pylint: disable=too-many-lines
"""
GUI for the PyBackup tool using Tkinter.

Provides a wizard-like interface for configuration, progress monitoring,
and viewing results. Interacts with pybackup_core running in a separate thread.
Includes internationalization support via gettext.
"""

# Standard library imports
import gettext
import locale
import logging
import os
import queue
import sys
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext, font
from typing import List, Dict, Any, Optional

# Local application imports
try:
    # Assumes pybackup_core.py is in the same directory or accessible via PYTHONPATH
    import pybackup_core
except ImportError as e:
    # Critical error if core logic cannot be imported
    print(f"Error: Could not import BackupGUI from pybackup_gui.py: {e}",
          file=sys.stderr)
    print("Please ensure main.py and pybackup_gui.py are in the same directory.",
          file=sys.stderr)
    sys.exit(1)
except Exception as e:  # pylint: disable=broad-except
    # Catch other potential errors during import
    print(f"An unexpected error occurred during import: {e}", file=sys.stderr)
    sys.exit(1)


# --- Internationalization (i18n) Setup ---

# Domain name for translation files (e.g., pybackup.mo)
APP_NAME = "pybackup"
# Path to the directory containing locale subdirectories (e.g., en/LC_MESSAGES)
# Uses absolute path for robustness, assumes 'locales' is in the same dir.
LOCALE_DIR = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), 'locales')

# Attempt to set the application's locale based on the system's default
languages = ['en']  # Initialize with default fallback language
try:
    # Setting LC_ALL to '' uses the system's default locale settings
    locale.setlocale(locale.LC_ALL, '')
    # Use getlocale() instead of deprecated getdefaultlocale()
    locale_code, _encoding_unused = locale.getlocale()
    # Use only the language part (e.g., 'en' from 'en_US') if available
    if locale_code:
        languages = [locale_code.split('_')[0]]
except (locale.Error, IndexError, ValueError, TypeError) as e:
    # Fallback gracefully if locale cannot be set or detected
    print(f"Warning: Could not set/detect system locale ({e}). Fallback 'en'.",
          file=sys.stderr)
    # languages remains ['en']

# Use basicConfig for initial logging setup before GUI possibly reconfigures it
# This ensures messages during i18n setup are logged somewhere.
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s') # NOSONAR

logging.info("Attempting language: %s. Locale dir: %s", languages, LOCALE_DIR)

# Set up the translation function using gettext
try:
    # Find and load the translation file (.mo) for the determined language(s)
    translation_obj = gettext.translation(
        APP_NAME, localedir=LOCALE_DIR, languages=languages, fallback=True
    )
    # Get the function that performs the translation lookup
    _ = translation_obj.gettext
    logging.info("Using language: %s",
                 languages[0] if languages else 'en (fallback)')
except FileNotFoundError:
    # Handle case where .mo files are missing even for the fallback
    logging.warning(
        "Locale files not found for %s at %s. Using default strings (English).",
        languages, LOCALE_DIR
    )
    _ = gettext.gettext  # Use a dummy function that returns the original string
except Exception as e:  # pylint: disable=broad-except
    # Catch any other errors during gettext setup
    logging.error("Error setting up gettext localization: %s", e)
    _ = gettext.gettext  # Fallback to original strings

# --- Default Configuration Constants ---
# These provide initial values shown in the GUI for configuration settings.

DEFAULT_LOG_FILE = 'pybackup.log'
DEFAULT_RESUME_STATE_FILE = 'pybackup.json'
DEFAULT_FREE_SPACE_PERCENTAGE = 10
DEFAULT_COPY_RETRIES = 3
DEFAULT_COPY_RETRY_DELAY_SECONDS = 10


# --- Main GUI Class ---

class PyBackupGUI:  # Renamed class
    """
    Manages the Tkinter GUI components and interaction logic for PyBackup.

    Creates the wizard pages (Configuration, Progress, Summary), handles
    user input, starts the backup process in a worker thread, and updates
    the UI based on messages received from the worker via queues.
    """

    def __init__(self, root: tk.Tk):
        """
        Initialize the main application window, variables, and pages.

        Args:
            root (tk.Tk): The main Tkinter window (root) instance.
        """
        self.root = root
        self.root.title(_("PyBackup"))
        # Define behavior when user tries to close the window (red X button)
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)

        # Use a slightly smaller font for description labels in options
        self.desc_font = font.Font(size=9)

        # --- Internal State Variables ---
        # Tkinter control variables linked directly to widgets
        self.source_dir = tk.StringVar()
        self.free_perc_var = tk.IntVar(value=DEFAULT_FREE_SPACE_PERCENTAGE)
        self.retries_var = tk.IntVar(value=DEFAULT_COPY_RETRIES)
        self.delay_var = tk.IntVar(value=DEFAULT_COPY_RETRY_DELAY_SECONDS)
        self.log_file_var = tk.StringVar(value=DEFAULT_LOG_FILE)
        self.state_file_var = tk.StringVar(value=DEFAULT_RESUME_STATE_FILE)

        # Internal list to store target directory paths added by the user
        self.target_dirs: List[str] = []
        # Dictionary holding the current configuration to pass to core logic
        self.config: Dict[str, Any] = {
            'free_percent': self.free_perc_var.get(),
            'retries': self.retries_var.get(),
            'delay': self.delay_var.get(),
            'log_file': self.log_file_var.get(),
            'state_file': self.state_file_var.get()
        }
        # Worker thread management
        self.backup_thread: Optional[threading.Thread] = None
        self.progress_queue: queue.Queue = queue.Queue()  # For progress/status
        self.log_queue: queue.Queue = queue.Queue()  # For log messages
        self.pause_event: threading.Event = threading.Event()  # To signal pause
        self.cancel_event: threading.Event = threading.Event()  # To signal cancel
        # Application status flags
        self.is_running: bool = False
        self.is_paused: bool = False
        # Tracks the target index for display purposes in the progress view
        self.current_processing_target_idx: int = 0

        # --- Wizard Page Setup ---
        self.current_page: int = 0
        self.pages: List[ttk.Frame] = []

        # Create frames for each step of the wizard interface
        self.page1_config = ttk.Frame(root, padding="10")
        self.page2_progress = ttk.Frame(root, padding="10")
        self.page3_summary = ttk.Frame(root, padding="10")
        self.pages.extend(
            [self.page1_config, self.page2_progress, self.page3_summary])

        # Build the UI content for each page
        self._create_page1_config()
        self._create_page2_progress()
        self._create_page3_summary()

        # Display the first page (configuration) initially
        self._show_page(0)
        # Start the periodic check for messages from the worker thread
        self.check_queues()

    def _show_page(self, page_index: int):
        """Hide all pages and display the page at the specified index."""
        if not 0 <= page_index < len(self.pages):
            logging.error("Invalid page index requested: %d", page_index)
            return
        for i, page in enumerate(self.pages):
            if i == page_index:
                # Make the requested page visible and allow it to expand
                page.pack(fill=tk.BOTH, expand=True)
            else:
                # Hide other pages
                page.pack_forget()
        self.current_page = page_index

    # --- Page 1: Configuration UI Creation ---
    def _create_page1_config(self):
        """Create and arrange widgets for the configuration page."""
        frame = self.page1_config
        # Allow the middle column (containing entries/listbox) to expand horizontally
        frame.columnconfigure(1, weight=1)

        # --- Source Directory Widgets ---
        ttk.Label(frame, text=_("Source Directory:")).grid(
            row=0, column=0, sticky=tk.W, padx=2, pady=3)
        ttk.Entry(frame, textvariable=self.source_dir, width=60).grid(
            row=0, column=1, sticky=(tk.W, tk.E), padx=5, pady=3)
        ttk.Button(frame, text=_("Browse..."), command=self._browse_source).grid(
            row=0, column=2, sticky=tk.W, padx=2, pady=3)

        # --- Target Directories List ---
        ttk.Label(frame, text=_("Target Directories:")).grid(
            row=1, column=0, sticky=(tk.N, tk.W), padx=2, pady=(10, 3))
        # Use an outer frame to contain the listbox and its scrollbars
        target_outer_frame = ttk.Frame(frame)
        target_outer_frame.grid(
            row=1, column=1, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=3)
        # Configure resizing behavior for the listbox area
        target_outer_frame.rowconfigure(0, weight=1)
        target_outer_frame.columnconfigure(0, weight=1)
        # Listbox widget to display target paths
        self.target_listbox = tk.Listbox(
            target_outer_frame, height=6, width=58, exportselection=False)
        self.target_listbox.grid(
            row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        # Scrollbars linked to listbox
        target_vsb = ttk.Scrollbar(
            target_outer_frame, orient="vertical", command=self.target_listbox.yview)
        target_vsb.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.target_listbox.configure(yscrollcommand=target_vsb.set)
        target_hsb = ttk.Scrollbar(
            target_outer_frame, orient="horizontal", command=self.target_listbox.xview)
        target_hsb.grid(row=1, column=0, sticky=(tk.W, tk.E))
        self.target_listbox.configure(xscrollcommand=target_hsb.set)
        # Frame for Add/Remove buttons next to the listbox
        target_btn_frame = ttk.Frame(frame)
        target_btn_frame.grid(
            row=1, column=2, sticky=(tk.N, tk.W), padx=2, pady=3)
        ttk.Button(target_btn_frame, text=_("Add..."),
                   command=self._add_target).pack(fill=tk.X, pady=2)
        ttk.Button(target_btn_frame, text=_("Remove"),
                   command=self._remove_target).pack(fill=tk.X, pady=2)
        # Allow the row containing the listbox setup to expand vertically
        frame.rowconfigure(1, weight=1)

        # --- Options Group ---
        opts_frame = ttk.LabelFrame(frame, text=_("Options"), padding="5")
        opts_frame.grid(
            row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), padx=2, pady=10)
        opts_frame.columnconfigure(1, minsize=60)  # Ensure space for Spinbox
        # Allow description label to expand
        opts_frame.columnconfigure(2, weight=1)

        # Option: Keep Free Space
        ttk.Label(opts_frame, text=_("Target Free Space (%):")).grid(
            row=0, column=0, sticky=tk.W, padx=5, pady=4)
        ttk.Spinbox(opts_frame, from_=0, to=90, increment=1, width=5,
                    textvariable=self.free_perc_var).grid(
                        row=0, column=1, sticky=tk.W, padx=5, pady=4)
        ttk.Label(opts_frame,
                  text=_("Minimum percentage of disk space to leave free (0-90)."),
                  font=self.desc_font, foreground="gray").grid(
                      row=0, column=2, sticky=tk.W, padx=5, pady=4)

        # Option: Copy Retries
        ttk.Label(opts_frame, text=_("Copy Retries:")).grid(
            row=1, column=0, sticky=tk.W, padx=5, pady=4)
        ttk.Spinbox(opts_frame, from_=0, to=10, increment=1, width=5,
                    textvariable=self.retries_var).grid(
                        row=1, column=1, sticky=tk.W, padx=5, pady=4)
        ttk.Label(opts_frame,
                  text=_(
                      "Number of times to retry copying a file after an error (0+)."),
                  font=self.desc_font, foreground="gray").grid(
                      row=1, column=2, sticky=tk.W, padx=5, pady=4)

        # Option: Retry Delay
        ttk.Label(opts_frame, text=_("Retry Delay (s):")).grid(
            row=2, column=0, sticky=tk.W, padx=5, pady=4)
        ttk.Spinbox(opts_frame, from_=0, to=600, increment=1, width=5,
                    textvariable=self.delay_var).grid(
                        row=2, column=1, sticky=tk.W, padx=5, pady=4)
        ttk.Label(opts_frame,
                  text=_("Seconds to wait between failed copy attempts (0+)."),
                  font=self.desc_font, foreground="gray").grid(
                      row=2, column=2, sticky=tk.W, padx=5, pady=4)

        # --- File Paths Group ---
        file_opts_frame = ttk.LabelFrame(frame, text=_("Files"), padding="5")
        file_opts_frame.grid(
            row=3, column=0, columnspan=3, sticky=(tk.W, tk.E), padx=2, pady=10)
        # Allow entry field to expand
        file_opts_frame.columnconfigure(1, weight=1)

        # Log File Path
        ttk.Label(file_opts_frame, text=_("Log File Path:")).grid(
            row=0, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Entry(file_opts_frame, textvariable=self.log_file_var, width=50).grid(
            row=0, column=1, sticky=(tk.W, tk.E), padx=5, pady=2)
        ttk.Button(file_opts_frame, text=_("..."), width=3,
                   command=self._browse_log_file).grid(
                       row=0, column=2, sticky=tk.W, padx=2)
        ttk.Label(file_opts_frame,
                  text=_("File where operation logs will be written."),
                  font=self.desc_font, foreground="gray").grid(
                      row=1, column=1, columnspan=2, sticky=tk.W, padx=5, pady=(0, 5))

        # State File Path
        ttk.Label(file_opts_frame, text=_("State File Path:")).grid(
            row=2, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Entry(file_opts_frame, textvariable=self.state_file_var, width=50).grid(
            row=2, column=1, sticky=(tk.W, tk.E), padx=5, pady=2)
        ttk.Button(file_opts_frame, text=_("..."), width=3,
                   command=self._browse_state_file).grid(
                       row=2, column=2, sticky=tk.W, padx=2)
        ttk.Label(file_opts_frame,
                  text=_(
                      "File used to store progress for resuming interrupted backups."),
                  font=self.desc_font, foreground="gray").grid(
                      row=3, column=1, columnspan=2, sticky=tk.W, padx=5, pady=(0, 5))

        # --- Navigation ---
        # Frame to hold the start button, aligned to the right
        nav_frame = ttk.Frame(frame)
        nav_frame.grid(row=4, column=0, columnspan=3, sticky=tk.E, pady=15)
        # Use default themed button
        start_button = ttk.Button(nav_frame, text=_(
            "Start Backup >"), command=self._start_backup)
        start_button.pack()

    def _browse_source(self):
        """Open directory dialog to select the source directory."""
        dir_path = filedialog.askdirectory(
            title=_("Select Source Directory"), mustexist=True
        )
        if dir_path:
            self.source_dir.set(dir_path)

    def _add_target(self):
        """Open directory dialog to add a target directory to the list."""
        dir_path = filedialog.askdirectory(
            title=_("Add Target Directory"), mustexist=True
        )
        # Add only if a path was selected and it's not already present
        if dir_path and dir_path not in self.target_dirs:
            self.target_dirs.append(dir_path)
            self.target_listbox.insert(tk.END, dir_path)  # Add to GUI listbox

    def _remove_target(self):
        """Remove selected target(s) from the listbox and internal list."""
        selected_indices = self.target_listbox.curselection()
        if not selected_indices:
            return  # Nothing selected
        # Iterate in reverse order to handle index changes correctly during deletion
        for i in sorted(selected_indices, reverse=True):
            try:
                del self.target_dirs[i]  # Remove from internal list
                self.target_listbox.delete(i)  # Remove from GUI listbox
            except IndexError:
                # Log error if index is somehow invalid during deletion
                logging.warning("Failed to remove target at index %d.", i)

    def _browse_log_file(self):
        """Open file dialog to select or specify a log file path."""
        path = filedialog.asksaveasfilename(
            title=_("Select Log File"),
            defaultextension=".log",
            initialfile=self.log_file_var.get(),  # Suggest current value
            filetypes=[(_("Log files"), "*.log"),
                       (_("Text files"), "*.txt"),
                       (_("All files"), "*.*")]
        )
        if path:
            self.log_file_var.set(path)

    def _browse_state_file(self):
        """Open file dialog to select or specify a state file path."""
        path = filedialog.asksaveasfilename(
            title=_("Select State File"),
            defaultextension=".json",
            initialfile=self.state_file_var.get(),  # Suggest current value
            filetypes=[(_("JSON files"), "*.json"),
                       (_("All files"), "*.*")]
        )
        if path:
            self.state_file_var.set(path)

    # --- Page 2: Progress Monitoring UI Creation ---
    def _create_page2_progress(self):
        """Create and arrange widgets for the progress monitoring page."""
        frame = self.page2_progress
        # Allow content to expand horizontally
        frame.columnconfigure(0, weight=1)

        # Frame for overall progress text display (items/size)
        info_frame = ttk.Frame(frame)
        info_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 5))
        ttk.Label(info_frame, text=_("Overall Progress:")).pack(side=tk.LEFT)
        self.progress_label = ttk.Label(
            info_frame, text=_("Starting..."))  # NOSONAR
        self.progress_label.pack(side=tk.LEFT, padx=5)

        # Frame for the Treeview (directory structure) and its scrollbars
        tree_frame = ttk.Frame(frame)
        tree_frame.grid(row=1, column=0, sticky=(
            tk.W, tk.E, tk.N, tk.S), pady=5)
        frame.rowconfigure(1, weight=3)  # Give tree area more vertical space
        tree_frame.rowconfigure(0, weight=1)  # Treeview row expands
        tree_frame.columnconfigure(0, weight=1)  # Treeview column expands

        # Treeview widget definition
        self.tree = ttk.Treeview(
            tree_frame, columns=("status", "target"), show="tree headings"
        )
        self.tree.heading("#0", text=_("Item Path"))  # Implicit tree column
        self.tree.heading("status", text=_("Status"))
        self.tree.heading("target", text=_("Current Target"))
        # Configure column properties (widths, stretching behavior)
        # Path column can stretch
        self.tree.column("#0", width=400, stretch=tk.YES)
        self.tree.column("status", width=150, anchor=tk.W, stretch=tk.NO)
        self.tree.column("target", width=200, anchor=tk.W, stretch=tk.NO)

        # Scrollbars linked to the Treeview
        tree_vsb = ttk.Scrollbar(
            tree_frame, orient="vertical", command=self.tree.yview)
        tree_hsb = ttk.Scrollbar(
            tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=tree_vsb.set,
                            xscrollcommand=tree_hsb.set)

        # Place Treeview and scrollbars using grid layout
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        tree_vsb.grid(row=0, column=1, sticky=(tk.N, tk.S))
        tree_hsb.grid(row=1, column=0, sticky=(tk.W, tk.E))

        # Dictionary mapping source paths to Treeview item IDs for efficient updates
        self.tree_item_map: Dict[str, str] = {}

        # Frame for the log display area
        log_frame = ttk.LabelFrame(frame, text=_("Logs"), padding="5")
        log_frame.grid(row=2, column=0, sticky=(
            tk.W, tk.E, tk.N, tk.S), pady=5)
        frame.rowconfigure(2, weight=1)  # Give log area some vertical weight
        log_frame.rowconfigure(0, weight=1)
        log_frame.columnconfigure(0, weight=1)  # Allow text widget to expand

        # ScrolledText widget handles its own scrollbars internally
        self.log_text = scrolledtext.ScrolledText(
            log_frame, height=8, wrap=tk.WORD, state=tk.DISABLED
        )
        # Make ScrolledText fill the available space in its grid cell
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Frame for control buttons (Pause, Cancel)
        control_frame = ttk.Frame(frame)
        control_frame.grid(row=3, column=0, sticky=(tk.W, tk.E), pady=(10, 0))

        self.pause_resume_btn = ttk.Button(control_frame, text=_("Pause"),
                                           command=self._toggle_pause, state=tk.DISABLED)
        self.pause_resume_btn.pack(side=tk.LEFT, padx=5)
        self.cancel_btn = ttk.Button(control_frame, text=_("Cancel"),
                                     command=self._cancel_backup, state=tk.DISABLED)
        self.cancel_btn.pack(side=tk.LEFT, padx=5)

    def _update_tree_display(self, source_path: Optional[str], status: str,
                             current_target_base_name: str = "",
                             full_destination_path: Optional[str] = None):
        """
        Update the Treeview to show only the active directory branch and status.

        Clears existing entries and rebuilds the path from the root source
        directory down to the currently processed item. Displays status and
        destination path information appropriately for leaf and parent nodes.

        Args:
            source_path: Absolute path of the source item being processed.
            status: Status string to display (should be translated).
            current_target_base_name: Base path of the current target (e.g., E:\\Backup).
            full_destination_path: Full destination path for the item (leaf node).
        """
        try:
            # Clear previous entries for simplicity in active-path display
            for i in self.tree.get_children():
                self.tree.delete(i)
            self.tree_item_map.clear()  # Reset path-to-ID map

            if not source_path or not self.source_dir.get():
                return  # Nothing to display

            abs_source_path = os.path.abspath(source_path)
            abs_source_root = os.path.abspath(self.source_dir.get())

            # --- Setup Root Node ---
            root_name = os.path.basename(abs_source_root) or abs_source_root
            root_iid = "root"  # Use a fixed, predictable ID for the root
            root_status = _("Processing...") if status else ""  # NOSONAR
            # Root node always shows the base target name in the target column
            root_values = (root_status, current_target_base_name)
            self.tree.insert(
                "", tk.END, iid=root_iid, text=root_name, open=True, values=root_values
            )
            self.tree_item_map[abs_source_root] = root_iid
            current_parent_iid = root_iid  # Start building path from root

            # If the item being processed IS the root, update it and finish
            if abs_source_path == abs_source_root:
                self.tree.item(root_iid, values=(
                    status, current_target_base_name))
                self.tree.see(root_iid)
                return

            # --- Build Path Nodes ---
            # Calculate relative path safely, handles path outside root error
            try:
                rel_path = os.path.relpath(abs_source_path, abs_source_root)
            except ValueError:
                # Log error and indicate issue in the tree root display
                logging.warning(
                    "Item path %s outside root %s.", abs_source_path, abs_source_root
                )
                self.tree.item(root_iid, values=(
                    _("Error path"), current_target_base_name))
                return

            parts = rel_path.split(os.sep)
            built_source_path = abs_source_root  # Tracks absolute source path being built

            # Insert/find nodes for the current path components
            for i, part in enumerate(parts):
                if not part:
                    # Skip potential empty parts from split (e.g., leading '/')
                    continue
                parent_iid = current_parent_iid  # Parent for the node being added/found
                built_source_path = os.path.join(
                    built_source_path, part)  # Update path for map key
                iid = self.tree_item_map.get(
                    built_source_path)  # Check if already mapped
                # Is this the final part (leaf node)?
                is_last = i == len(parts) - 1

                # Insert node if it doesn't exist in the map/tree yet
                if not iid or not self.tree.exists(iid):
                    iid = self.tree.insert(
                        parent_iid, tk.END, text=part, open=True)
                    # Store new ID in map
                    self.tree_item_map[built_source_path] = iid
                else:
                    # Ensure existing node is expanded
                    self.tree.item(iid, open=True)

                # Determine Status and Target Text for this Node
                node_status = status if is_last else _("Processing...")
                if is_last:
                    # Leaf node: Use the full destination path if provided, else base name
                    node_target = full_destination_path if full_destination_path \
                        else current_target_base_name
                else:
                    # Intermediate node: Construct its corresponding destination path
                    try:
                        # Get parts up to current index
                        current_rel_parts = parts[:i+1]
                        current_rel_path_for_node = os.path.join(
                            *current_rel_parts)
                        node_target = os.path.join(
                            current_target_base_name, current_rel_path_for_node
                        )
                    except (OSError, ValueError) as path_err:
                        # Fallback to base name if intermediate path calculation fails
                        logging.warning(
                            "Could not calculate intermediate dest path: %s", path_err
                        )
                        node_target = current_target_base_name
                # Update the values shown in the Treeview columns for this item
                self.tree.item(iid, values=(node_status, node_target))
                current_parent_iid = iid  # The current node becomes parent for the next part

            # Ensure the last item (leaf node) is visible after update
            if current_parent_iid:
                self.tree.see(current_parent_iid)

        except tk.TclError as e:
            # Catch Tkinter errors gracefully if widget becomes invalid during update
            logging.warning("TclError updating tree display: %s", e)
        except Exception as e:  # pylint: disable=broad-except
            # Catch any other unexpected errors during tree update
            logging.exception("Error updating tree display: %s", e)

    # --- Page 3: Summary ---

    def _create_page3_summary(self):
        """Create and arrange widgets for the summary page."""
        frame = self.page3_summary
        frame.columnconfigure(0, weight=1)  # Allow content to expand

        ttk.Label(frame, text=_("Backup Summary"), font="-weight bold").grid(
            row=0, column=0, pady=10)
        self.summary_label = ttk.Label(
            frame, text=_("Backup finished/cancelled."))
        self.summary_label.grid(row=1, column=0, pady=5, sticky=tk.W)
        # Label to display final item/size statistics
        self.stats_label = ttk.Label(frame, text="")
        self.stats_label.grid(row=2, column=0, pady=5, sticky=tk.W)

        # Section for displaying failed items
        ttk.Label(frame, text=_("Failed Items:")).grid(
            row=3, column=0, pady=(10, 0), sticky=tk.W)
        self.failed_text = scrolledtext.ScrolledText(
            frame, height=10, wrap=tk.WORD, state=tk.DISABLED)
        self.failed_text.grid(row=4, column=0, sticky=(
            tk.W, tk.E, tk.N, tk.S), pady=5)
        # Allow failed text area to expand vertically
        frame.rowconfigure(4, weight=1)

        # Frame for the final Close button
        nav_frame = ttk.Frame(frame)
        nav_frame.grid(row=5, column=0, sticky=tk.E, pady=10)
        # Use default button style
        ttk.Button(nav_frame, text=_("Close"),
                   command=self.root.destroy).pack()

    # --- Actions and Event Handlers ---

    def _validate_config(self) -> bool:
        """Validate user inputs on the configuration page before starting."""
        source = self.source_dir.get()
        if not source or not os.path.isdir(source):
            messagebox.showerror(
                _("Error"), _("Please select a valid source directory.")
            )
            return False
        if not self.target_dirs:
            messagebox.showerror(
                _("Error"), _("Please add at least one target directory.")
            )
            return False

        # Validate target directories, attempt creation if confirmed by user
        validated_targets = []
        for target in self.target_dirs:
            abs_target = os.path.abspath(target)  # Ensure absolute paths
            if not os.path.isdir(abs_target):
                # Ask user if non-existent target should be created
                q_title = _("Confirm Target Creation")
                q_msg = _(
                    "Target directory '{path}' does not exist.\n\n"
                    "Do you want to create it?"
                ).format(path=abs_target)
                if messagebox.askyesno(q_title, q_msg, icon='warning'):
                    try:
                        # Attempt to create directory
                        os.makedirs(abs_target, exist_ok=True)
                        validated_targets.append(abs_target)
                    except OSError as e:
                        # Catch specific OS errors during creation
                        msg = _("Could not create target '{path}': {error}").format(
                            path=abs_target, error=e
                        )
                        messagebox.showerror(_("Error"), msg)
                        return False
                    except Exception as e:  # pylint: disable=broad-except
                        # Catch other potential errors
                        msg = _("Could not create target '{path}': {error}").format(
                            path=abs_target, error=e
                        )
                        messagebox.showerror(_("Error"), msg)
                        return False
                else:
                    # User chose not to create
                    msg = _("Target directory '{path}' must exist.").format(
                        path=abs_target)
                    messagebox.showerror(_("Error"), msg)
                    return False
            else:
                # Check writability if directory already exists
                if not os.access(abs_target, os.W_OK):
                    msg = _("Target directory '{path}' is not writable.").format(
                        path=abs_target)
                    messagebox.showerror(_("Error"), msg)
                    return False
                validated_targets.append(abs_target)
        # Store the validated, absolute target paths
        self.target_dirs = validated_targets

        # Validate numeric options and file paths from GUI variables
        try:
            # Update internal config dict from Tkinter variables
            self.config['free_percent'] = self.free_perc_var.get()
            self.config['retries'] = self.retries_var.get()
            self.config['delay'] = self.delay_var.get()
            self.config['log_file'] = self.log_file_var.get()
            self.config['state_file'] = self.state_file_var.get()

            # Perform range/validity checks
            if not self.config['log_file']:
                raise ValueError(_("Log file path empty."))
            if not self.config['state_file']:
                raise ValueError(_("State file path empty."))
            if not 0 <= self.config['free_percent'] <= 90:
                raise ValueError(_("Free percent must be 0-90."))
            if self.config['retries'] < 0:
                raise ValueError(_("Retries >= 0."))
            if self.config['delay'] < 0:
                raise ValueError(_("Retry delay >= 0."))

        except (ValueError, tk.TclError) as e:
            # Catch errors from tk.IntVar.get() or explicit ValueErrors
            msg = _("Invalid option value: {error}").format(error=e)
            messagebox.showerror(_("Error"), msg)
            return False
        # All checks passed
        return True

    def _start_backup(self):
        """Validate config and start the backup process in a worker thread."""
        # Prevent starting if validation fails or already running
        if not self._validate_config() or self.is_running:
            return

        # Set running state and reset controls/events
        self.is_running = True
        self.is_paused = False
        self.pause_event.clear()
        self.cancel_event.clear()
        self.current_processing_target_idx = 0  # Start with first target index

        # Update button states for progress page
        self.pause_resume_btn.config(text=_("Pause"), state=tk.NORMAL)
        self.cancel_btn.config(text=_("Cancel"), state=tk.NORMAL)

        # Reset progress/summary displays for new run
        initial_target_name = self.target_dirs[0] if self.target_dirs else ""
        self._update_tree_display(
            self.source_dir.get(), _("Starting..."), initial_target_name
        )
        self.progress_label.config(text=_("Starting..."))
        # Clear log and failed items text areas safely
        for text_widget in [self.log_text, self.failed_text]:
            try:
                text_widget.config(state=tk.NORMAL)
                text_widget.delete('1.0', tk.END)
                text_widget.config(state=tk.DISABLED)
            except tk.TclError:
                pass  # Ignore if widget destroyed

        self.summary_label.config(text=_("Backup in progress..."))
        self.stats_label.config(text="")

        # --- Reconfigure File Logging for this Run ---
        log_level = logging.getLogger().level  # Preserve current log level
        # Remove previous file/stream handlers to avoid duplication or permission issues
        logger = logging.getLogger()  # Use root logger
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()
        # Setup logging again with potentially new file path from config
        try:
            # Use basicConfig (convenience function) - needs force=True for reconfig
            logging.basicConfig(
                level=log_level,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(
                        self.config['log_file'], mode='w', encoding='utf-8'),
                    logging.StreamHandler(sys.stdout)  # Keep console output
                ],
                force=True  # Allow reconfiguring root logger in Python 3.8+
            )
            logging.info("Logging reconfigured to file: %s",
                         self.config['log_file'])
        except IOError as e:
            # Catch specific file errors
            print(f"Error setting up log file handler for {self.config['log_file']}: {e}",
                  file=sys.stderr)
            # Fallback to console only if file logging fails
            logging.basicConfig(
                level=log_level, format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[logging.StreamHandler(sys.stdout)], force=True)
        except Exception as e:  # pylint: disable=broad-except
            # Catch other potential errors
            print(f"Error re-setting log file handler: {e}", file=sys.stderr)
            logging.basicConfig(
                level=log_level, format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[logging.StreamHandler(sys.stdout)], force=True)

        # Switch view to progress page and log start
        self._show_page(1)
        self._add_log(logging.INFO, _("Backup started."))

        # --- Create and Start the Worker Thread ---
        self.backup_thread = threading.Thread(
            target=pybackup_core.start_backup_session,
            args=(  # Pass validated config and communication objects
                self.source_dir.get(),
                # Pass copy of validated absolute paths
                list(self.target_dirs),
                self.config,
                self.config['state_file'],
                self.progress_queue,
                self.log_queue,
                self.pause_event,
                self.cancel_event
            ),
            daemon=True  # Allows main GUI thread to exit even if worker is stuck
        )
        self.backup_thread.start()

    def _toggle_pause(self):
        """Toggle the pause/resume state of the backup process."""
        if not self.is_running:
            return  # Do nothing if not running
        if self.is_paused:
            # Resume the backup
            self.pause_event.clear()  # Signal worker thread to continue
            self.pause_resume_btn.config(text=_("Pause"))
            self.is_paused = False
            self._add_log(logging.INFO, _("Backup Resumed."))
        else:
            # Pause the backup
            self.pause_event.set()  # Signal worker thread to pause
            self.pause_resume_btn.config(text=_("Resume"))
            self.is_paused = True
            self._add_log(logging.INFO, _("Backup Paused."))

    def _cancel_backup(self):
        """Request cancellation of the backup process after confirmation."""
        if not self.is_running:
            return  # Do nothing if not running
        # Ask user for confirmation
        if messagebox.askyesno(
            _("Cancel Backup"), _("Cancel backup process?"), icon='warning'
        ):
            self.cancel_event.set()  # Signal worker thread to cancel
            # Update GUI immediately for responsiveness
            self.pause_resume_btn.config(state=tk.DISABLED)
            self.cancel_btn.config(text=_("Cancelling..."), state=tk.DISABLED)
            self._add_log(logging.WARNING, _("Cancellation requested..."))

    def _add_log(self, level: int, message: str):
        """Add a log message originating from the GUI thread to the queue."""
        # Prefix with [GUI] to distinguish from core logic logs if desired
        self.log_queue.put({'level': level, 'message': f"[GUI] {message}"})

    def check_queues(self):
        """Periodically check the progress and log queues for messages."""
        # Process all available messages in the progress queue non-blockingly
        try:
            while True:
                message = self.progress_queue.get_nowait()
                self.handle_progress_message(message)
        except queue.Empty:
            pass  # No progress messages currently

        # Process all available messages in the log queue non-blockingly
        try:
            while True:
                log_record = self.log_queue.get_nowait()
                self.display_log_message(log_record)
        except queue.Empty:
            pass  # No log messages currently

        # Reschedule this check if the main window still exists
        # This forms the basis of the GUI's responsiveness while the worker runs
        if self.root.winfo_exists():
            self.root.after(100, self.check_queues)  # Check again in 100ms

    def handle_progress_message(self, message: Dict[str, Any]):
        """Process messages received from the backup worker thread via queue."""
        msg_type = message.get('type')

        # Determine the current target directory name safely for display
        current_target_name = "N/A"  # Default if index is somehow invalid
        if 0 <= self.current_processing_target_idx < len(self.target_dirs):
            current_target_name = self.target_dirs[self.current_processing_target_idx]

        # Extract common payload elements
        full_dest_path = message.get('destination_path')
        status_text = message.get('message', '')
        source_path = message.get('source_path')  # May be None

        # --- Process different message types ---
        if msg_type == 'status':
            # Status messages update the tree display with activity info
            path_for_status = message.get(
                'item') or message.get('current_dir', '')
            # Map known internal status texts to translated UI texts
            status_map = {
                "Scanning/Comparing...": _("Scanning/Comparing..."),
                "Deleting...": _("Deleting..."),
                "Entering dir...": _("Entering dir..."),
                "Processing link...": _("Processing link..."),
                "Processing file...": _("Processing file..."),
                "Copying...": _("Copying..."),
                "Copying ({size})...": _("Copying ({size})...").format(
                    size=message.get('size_hr', '')),
                "Directory done.": _("Directory done."),
                "Scanning done.": _("Scanning done.")
            }
            display_status = status_map.get(
                status_text, status_text)  # Fallback
            # Pass full dest path only if the status relates to a specific item
            dest_for_display = full_dest_path if message.get('item') else None
            if path_for_status:
                self._update_tree_display(
                    path_for_status, display_status, current_target_name,
                    dest_for_display
                )

        elif msg_type == 'item_start':
            # Indicates processing starts for a specific item
            if source_path:
                self._update_tree_display(
                    source_path, _("Processing..."), current_target_name,
                    full_dest_path
                )

        elif msg_type == 'item_done':
            # Indicates processing finished for a specific item (file/link)
            status = _("Done") if message.get('success') else _("Failed")
            if source_path:
                self._update_tree_display(
                    source_path, status, current_target_name, full_dest_path
                )

        elif msg_type == 'progress_update':
            # Updates overall statistics display (items and size)
            total_items = message.get('items_processed', 0)
            total_bytes = message.get('size_copied', 0)
            progress_str = _("{items} items ({size})").format(
                items=total_items, size=self._human_readable_size(total_bytes)
            )
            self.progress_label.config(text=progress_str)

        elif msg_type == 'target_switch':
            # Updates the GUI's tracking of the current target index
            self.current_processing_target_idx = message.get('index', 0)
            # current_target_name used in display will update on the next message

        elif msg_type == 'done' or msg_type == 'error' or msg_type == 'cancelled':
            # Handles completion, cancellation, or fatal error from worker thread
            self.is_running = False
            self.is_paused = False
            # Ensure controls are disabled on final page
            self.pause_resume_btn.config(state=tk.DISABLED)
            self.cancel_btn.config(
                text=_("Cancel"), state=tk.DISABLED)  # Reset text

            # Set summary message based on outcome
            summary_msg = _("Backup Finished Successfully.")
            if msg_type == 'error':
                err_msg = message.get('message', _('Unknown error'))
                summary_msg = _("Backup Failed: {error}").format(error=err_msg)
            if msg_type == 'cancelled':
                summary_msg = _("Backup Cancelled by user.")
            self.summary_label.config(text=summary_msg)

            # Display final statistics for the run
            total_items = message.get('total_items', 0)
            total_size = message.get('total_size', 0)
            failed = message.get('failed_items', [])
            stats_display = _(
                "Items processed (run): {items}\n"
                "Data copied (run): {size}"
            ).format(
                items=total_items, size=self._human_readable_size(total_size)
            )
            self.stats_label.config(text=stats_display)

            # Display list of failed items
            self.failed_text.config(state=tk.NORMAL)
            self.failed_text.delete('1.0', tk.END)
            if failed:
                self.failed_text.insert(
                    tk.END, _("Path: Reason") + "\n" + "="*20 + "\n")
                for item, reason in failed:
                    # Ensure reason is safely converted to string
                    reason_str = str(
                        reason) if reason is not None else _("Unknown")
                    self.failed_text.insert(tk.END, f"{item}: {reason_str}\n")
            else:
                self.failed_text.insert(tk.END, _("None") + "\n")
            self.failed_text.config(state=tk.DISABLED)

            # Automatically switch view to the summary page
            self._show_page(2)

    def display_log_message(self, log_record: Dict[str, Any]):
        """Append a log message to the Text widget in a thread-safe way."""
        message = log_record.get('message', '')
        try:
            # Ensure widget updates happen in the main GUI thread
            self.log_text.config(state=tk.NORMAL)  # Must be normal to insert
            self.log_text.insert(tk.END, message + '\n')
            self.log_text.see(tk.END)  # Auto-scroll to the latest message
            self.log_text.config(state=tk.DISABLED)  # Set back to disabled
        except tk.TclError:
            pass  # Ignore errors if widget is destroyed during application shutdown
        except Exception as e:  # pylint: disable=broad-except
            # Log errors occurring during log display to stderr as a fallback
            # Using broad except here as various Tkinter issues could occur
            print(f"Error displaying log message in GUI: {e}", file=sys.stderr)

    def _on_closing(self):
        """Handle the window close event (WM_DELETE_WINDOW)."""
        if self.is_running:
            # Ask for confirmation if backup is running
            title = _("Exit Confirmation")
            msg = _(
                "Backup is in progress. Are you sure you want to exit?\n"
                "This will cancel the backup."
            )
            if messagebox.askyesno(title, msg, icon='warning'):
                self.cancel_event.set()  # Signal worker thread to cancel
                self.root.destroy()  # Close the window
            # else: User clicked No, do nothing
        else:
            # If not running, close window immediately
            self.root.destroy()

    # Static method helper for consistent size formatting within the GUI
    @staticmethod
    def _human_readable_size(size_bytes: float) -> str:
        """Convert bytes to a human-readable string (KB, MB, GB, TB)."""
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = 0
        while size_bytes >= 1024 and i < len(size_name) - 1:
            size_bytes /= 1024.0
            i += 1
        return f"{size_bytes:.2f} {size_name[i]}"

# -*- coding: utf-8 -*-
"""
Main entry point for the PyBackup GUI application.

Initializes the Tkinter environment, attempts to apply a suitable
theme, creates the main application window using the PyBackupGUI class from
pybackup_gui.py, and starts the Tkinter event loop.
"""

# Standard library imports
import tkinter as tk
from tkinter import ttk
import sys
import logging  # Added import

# Local application imports
# Ensure pybackup_gui module and PyBackupGUI class are found.
# Assumes pybackup_gui.py is in the same directory or in the Python path.
try:
    from pybackup_gui import PyBackupGUI
except ImportError as e:
    # Provide helpful error message if the GUI module cannot be imported
    print(
        f"Error: Could not import PyBackupGUI from pybackup_gui.py: {e}", file=sys.stderr)
    print("Please ensure main.py and pybackup_gui.py are in the same directory.", file=sys.stderr)
    sys.exit(1)  # Exit with an error code
except Exception as e:  # pylint: disable=broad-except
    # Catch other potential errors during import (less common)
    # Log critical error before exiting if logging is available
    logging.basicConfig(level=logging.ERROR,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.critical(
        "An unexpected error occurred during GUI import: %s", e, exc_info=True)
    print(f"An unexpected error occurred during import: {e}", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    # Basic logging configuration as a fallback if GUI/core setup fails
    # The GUI will reconfigure this later with the chosen log file
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    # Create the main application window (root)
    root = tk.Tk()

    # --- Optional: Apply a ttk theme for a more modern look ---
    # This attempts to use platform-native or good cross-platform themes
    # if available, falling back gracefully if themes are missing or cause errors.
    try:
        style = ttk.Style()
        available_themes = style.theme_names()
        logging.debug("Available ttk themes: %s", available_themes)

        # Platform-specific theme preferences
        if sys.platform == "win32" and 'vista' in available_themes:
            style.theme_use('vista')
            logging.debug("Applying 'vista' theme for Windows.")
        elif sys.platform == "darwin" and 'aqua' in available_themes:
            style.theme_use('aqua')
            logging.debug("Applying 'aqua' theme for macOS.")
        # Good cross-platform fallbacks
        elif 'clam' in available_themes:
            style.theme_use('clam')
            logging.debug("Applying 'clam' theme.")
        elif 'alt' in available_themes:
            style.theme_use('alt')
            logging.debug("Applying 'alt' theme.")
        # 'default' and 'classic' are usually available but look older
    except Exception as e:  # pylint: disable=broad-except
        # Ignore theme errors and let Tkinter use its default theme
        logging.warning("Could not apply custom ttk theme: %s", e)
        print(f"Note: Could not apply custom ttk theme: {e}", file=sys.stderr)

    # --- Create and run the GUI application instance ---
    try:
        # Instantiate the main GUI class, passing the root window
        app = PyBackupGUI(root)
        # Start the Tkinter event loop (makes the window interactive)
        root.mainloop()
    except Exception as e:  # pylint: disable=broad-except
        # Catch potential errors during GUI initialization or the main loop
        # Use logging.exception to include traceback information
        logging.exception("An error occurred running the application: %s", e)
        # Also print a user-friendly message to stderr
        print(
            f"An error occurred running the application: {e}", file=sys.stderr)
        sys.exit(1)  # Exit with a general error code

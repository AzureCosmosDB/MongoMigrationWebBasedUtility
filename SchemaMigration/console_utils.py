"""
Shared console utilities for colored terminal output.

This module provides ANSI color codes and helper functions for printing
colored messages (success, warning, error) to the terminal.
"""

# ANSI color codes for terminal output
class Colors:
    YELLOW = '\033[93m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    RESET = '\033[0m'


def print_warning(message: str) -> None:
    """Print a warning message in yellow."""
    print(f"{Colors.YELLOW}{message}{Colors.RESET}")


def print_error(message: str) -> None:
    """Print an error message in red."""
    print(f"{Colors.RED}{message}{Colors.RESET}")


def print_success(message: str) -> None:
    """Print a success/milestone message in green."""
    print(f"{Colors.GREEN}{message}{Colors.RESET}")


def print_verbose(verbose: bool, message: str) -> None:
    """Print a message if verbose mode is enabled."""
    if verbose:
        print(message)

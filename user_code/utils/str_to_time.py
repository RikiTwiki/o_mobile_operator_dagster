# utils/str_to_time.py (Revised)

from datetime import datetime
from dateutil import parser

def str_to_datetime_object(s: str) -> datetime | None: # Can return None if parsing fails
    """
    Converts a datetime string to a datetime object.
    Handles various ISO 8601 formats and common YYYY-MM-DD HH:MM:SS format.
    Returns None if parsing fails.
    """
    if not isinstance(s, str) or not s: # Handle non-string or empty inputs
        return None
    try:
        return parser.parse(s)
    except (ValueError, TypeError):
        # Log this if you want to know which strings fail parsing
        # print(f"Warning: Could not parse datetime string: '{s}'")
        return None

def str_to_time(s: str) -> int | None:
    """
    Converts a datetime string to a Unix timestamp (integer seconds).
    Returns None if parsing fails.
    """
    dt_object = str_to_datetime_object(s)
    if dt_object:
        return int(dt_object.timestamp())
    return None
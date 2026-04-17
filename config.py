"""
Configuration for the Earnings Intelligence System.

Loads environment variables, defines paths, constants, and logging setup.
All configuration is centralized here — other modules import from this file.
"""

import os
import logging
from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

load_dotenv()

# Finnhub
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

# Google Calendar
GOOGLE_CALENDAR_ID = os.getenv("GOOGLE_CALENDAR_ID")
GOOGLE_CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    str(Path(__file__).parent / "credentials.json"),
)

# Coverage Manager (source of truth for tickers and tiers)
COVERAGE_MANAGER_PATH = os.getenv(
    "COVERAGE_MANAGER_PATH",
    str(Path(__file__).parent.parent / "Coverage Manager"),
)

# Timezone
TIMEZONE = os.getenv("TIMEZONE", "America/New_York")

# Notifications
SLACK_WEBHOOK_EARNINGS = os.getenv("SLACK_WEBHOOK_EARNINGS")
EMAIL_TO = os.getenv("EMAIL_TO")

# Database
DB_PATH = Path(__file__).parent / "earnings_events.db"

# Where --weekly-digest writes rendered HTML for Gmail MCP drafting
DIGEST_HTML_PATH = Path(__file__).parent / "last_digest.html"

# Legacy tickers file (fallback only — Coverage Manager is preferred)
TICKERS_FILE = Path(__file__).parent / "tickers.txt"

# ---------------------------------------------------------------------------
# Finnhub query settings
# ---------------------------------------------------------------------------

CHUNK_DAYS = 7          # Days per Finnhub earnings calendar query
CHUNK_SLEEP = 1         # Seconds between Finnhub API chunks
FINNHUB_MAX_RESULTS = 1500  # Finnhub's per-query result cap

# ---------------------------------------------------------------------------
# Retry settings
# ---------------------------------------------------------------------------

RETRY_MAX_ATTEMPTS = 3
RETRY_BASE_DELAY = 2    # Seconds; doubles each retry (2, 4, 8)

# ---------------------------------------------------------------------------
# Google Calendar API
# ---------------------------------------------------------------------------

CALENDAR_SCOPES = ["https://www.googleapis.com/auth/calendar.events"]
CALENDAR_PAGE_SIZE = 250  # Max results per Calendar API page

# ---------------------------------------------------------------------------
# Timing labels
# ---------------------------------------------------------------------------

TIMING_LABELS = {
    "bmo": "Before Market Open",
    "amc": "After Market Close",
    "dmh": "During Market Hours",
}

# ---------------------------------------------------------------------------
# Tier definitions
# ---------------------------------------------------------------------------

TIER_1_LABEL = "Core Watchlist"
TIER_2_LABEL = "HC Services + MedTech"
TIER_3_LABEL = "Other"

TIER_2_SECTORS = {"Healthcare Services", "MedTech"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("earnings_agent")

# Suppress noisy Google API cache warning
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)

"""One-time Gmail authorization for the IR-alerts scanner.

Run this ONCE on your local machine after dropping
`gmail_client_credentials.json` into the project root. It opens a
browser, asks you to sign in as floridabusinessman@gmail.com, and
saves a refresh token to `gmail_token.json` that the agent uses
non-interactively from then on.

Usage:
    cd earnings_agent
    python scripts/authorize_gmail.py

The script picks a free local port, opens the consent screen, and
captures the redirect. If the browser doesn't open automatically,
copy the printed URL into a browser manually.

After it succeeds:
- gmail_token.json is in .gitignore — keep it local
- For GitHub Actions, paste the file's contents into a new repo
  secret named GMAIL_TOKEN_JSON; the workflow will write it to
  disk before running the agent
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    print(
        "Missing dependency. Install with:\n"
        "    pip install google-auth-oauthlib"
    )
    sys.exit(1)


# Read-only scope — minimal access required to scan IR alert emails.
# Don't broaden to gmail.modify or gmail.send unless we add features
# that need them; downstream auditing is easier with the tightest scope.
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

ROOT = Path(__file__).resolve().parent.parent
CLIENT_FILE = ROOT / "gmail_client_credentials.json"
TOKEN_FILE = ROOT / "gmail_token.json"


def main() -> int:
    if not CLIENT_FILE.exists():
        print(
            f"ERROR: {CLIENT_FILE.name} not found in {ROOT}\n\n"
            "Steps to obtain it:\n"
            "  1. Open https://console.cloud.google.com\n"
            "  2. Pick your earnings-agent project\n"
            "  3. APIs & Services → Credentials → Create Credentials → OAuth client ID\n"
            "  4. Application type: Desktop app\n"
            "  5. Download the JSON and save it as gmail_client_credentials.json\n"
        )
        return 1

    if TOKEN_FILE.exists():
        ans = input(
            f"{TOKEN_FILE.name} already exists. Overwrite? [y/N] "
        ).strip().lower()
        if ans != "y":
            print("Aborted.")
            return 0

    flow = InstalledAppFlow.from_client_secrets_file(
        str(CLIENT_FILE), SCOPES
    )
    print(
        "Opening browser to authorize. Sign in as the Gmail address "
        "you want to scan (e.g. floridabusinessman@gmail.com)."
    )
    creds = flow.run_local_server(port=0, open_browser=True)
    TOKEN_FILE.write_text(creds.to_json(), encoding="utf-8")
    print(f"\nSaved refresh token to {TOKEN_FILE}")
    print(
        "\nNext: for GitHub Actions, run:\n"
        f"    gh secret set GMAIL_TOKEN_JSON -R jroypeterson/earnings-agent < {TOKEN_FILE}\n"
        "    gh secret set GMAIL_CLIENT_CREDENTIALS_JSON -R jroypeterson/earnings-agent "
        f"< {CLIENT_FILE}\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

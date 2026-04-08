# Earnings Call Calendar Agent

Automatically monitors upcoming earnings calls for your watchlist and adds them to Google Calendar. Powered by Finnhub's free API.

## What It Does

- Queries Finnhub daily for upcoming earnings dates across your ticker watchlist
- Creates Google Calendar events with timing info (before market open / after market close)
- Includes consensus EPS and revenue estimates in the event description
- Deduplicates via SQLite — won't create duplicate events on reruns
- Supports dry-run mode for testing

## Quick Start (Local)

### 1. Get API Keys

**Finnhub** (free):
1. Register at https://finnhub.io/register
2. Copy your API key from the dashboard

**Google Calendar API** (free):
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project (or use an existing one)
3. Enable the **Google Calendar API**
4. Create a **Service Account** (IAM & Admin → Service Accounts)
5. Create a key for the service account → download the JSON file as `credentials.json`
6. In Google Calendar, go to your "Public Investing" calendar → Settings → Share with specific people
7. Add the service account email (looks like `name@project.iam.gserviceaccount.com`) with **Make changes to events** permission
8. Copy the **Calendar ID** from the calendar settings (looks like `abc123@group.calendar.google.com`)

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your values:
#   FINNHUB_API_KEY=...
#   GOOGLE_CALENDAR_ID=...
#   GOOGLE_CREDENTIALS_PATH=credentials.json
#   TICKERS=UNH,CI,HUM,ELV,CNC,MOH,CVS,HCA,THC,UHS
```

### 3. Install & Run

```bash
pip install -r requirements.txt

# Preview what would be added (no calendar changes)
python earnings_agent.py --dry-run

# Actually create the events
python earnings_agent.py

# Also check past 30 days for anything missed
python earnings_agent.py --backfill
```

## Deploy on GitHub Actions (Free Daily Runs)

This is the recommended way to run the agent automatically.

### 1. Push to GitHub

```bash
git init && git add . && git commit -m "Initial commit"
gh repo create earnings-agent --public --push
```

### 2. Add Secrets & Variables

In your repo → Settings → Secrets and variables → Actions:

**Secrets** (encrypted):
| Name | Value |
|------|-------|
| `FINNHUB_API_KEY` | Your Finnhub API key |
| `GOOGLE_CALENDAR_ID` | Your calendar ID |
| `GOOGLE_CREDENTIALS_JSON` | Entire contents of your `credentials.json` file |

**Variables** (visible, easy to edit):
| Name | Value |
|------|-------|
| `TICKERS` | `UNH,CI,HUM,ELV,CNC,MOH,CVS,HCA,THC,UHS` |

### 3. Enable the Workflow

Go to repo → Actions tab → enable workflows. The agent will run daily at ~6 AM ET. You can also trigger it manually via the "Run workflow" button.

## Project Structure

```
earnings-agent/
├── earnings_agent.py              # Main script
├── requirements.txt               # Python dependencies
├── .env.example                   # Config template
├── .gitignore
├── .github/
│   └── workflows/
│       └── daily_earnings_check.yml   # GitHub Actions daily schedule
└── README.md
```

## Customization

**Add/remove tickers**: Edit the `TICKERS` variable in `.env` or GitHub Actions variables.

**Change the look-ahead window**: In `earnings_agent.py`, adjust the `timedelta(days=90)` in the `run()` function.

**Change reminder timing**: Modify the `reminders` dict in `create_calendar_event()`. The defaults are 1 hour before for timed events, 12 hours before for all-day events.

**Event colors**: Add `"colorId": "11"` (tomato red) or other color IDs to the event body in `create_calendar_event()`. See [Google Calendar color IDs](https://lukeboyle.com/blog/posts/google-calendar-api-color-id).

## Finnhub Free Tier Limits

- 60 API calls/minute
- The earnings calendar endpoint returns all earnings in a date range, so one call covers your entire watchlist
- More than sufficient for daily runs

## Future Enhancements

- [ ] Add conference attendance tracking via EDGAR 8-K filings
- [ ] Slack/email notifications when new earnings are detected
- [ ] Track estimate revisions and update calendar descriptions
- [ ] Add actual vs. estimate results after earnings are reported

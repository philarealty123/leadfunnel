# Lead Funnel Pipeline

Philadelphia public-record lead collection and Google Sheets export.

## Operations

- Scheduled for weekdays at 11:00 UTC (6:00 AM EST / 7:00 AM EDT).
- Active adapters are configured in `config/sources.yaml`.
- Each run writes a non-sensitive `data/run_summary.json` health report.
- A failure in one source no longer blocks healthy sources from completing.
- The SQLite cache uses a new immutable key per run and restores the newest prior database.

Required GitHub Actions secrets:

- `GOOGLE_CREDENTIALS_JSON`: service-account JSON with access to the destination spreadsheet.
- `GOOGLE_SHEET_ID`: destination spreadsheet ID.

Run locally with `python main.py run --dry-run`. Never commit credentials or the lead database.

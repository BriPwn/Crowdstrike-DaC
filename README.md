# CrowdStrike Detection as Code — Per-Rule File Pipeline

A Detection-as-Code pipeline for managing CrowdStrike Correlation Rules through
version control.  Each rule lives in its own JSON file under `rules/`, making
individual rule changes clearly visible in Git diffs and pull requests.

Adapted from the [CrowdStrike FalconPy detection_as_code sample](https://github.com/CrowdStrike/falconpy/tree/main/samples/correlation_rules/detection_as_code).

---

## Repository structure

```
.
├── .github/
│   └── workflows/
│       ├── update_correlation_rules.yml   # Push-triggered sync
│       └── on_demand_sync.yml             # Manual full sync from API
├── rules/
│   ├── My_Detection_Rule.json             # One file per rule
│   ├── Kerberoasting_Detector.json
│   └── ...
├── scripts/
│   └── sync_detections.py
├── requirements.txt
└── README.md
```

---

## Requirements

- Python 3.x
- `crowdstrike-falconpy >= 1.4.8`

```
pip install -r requirements.txt
```

---

## GitHub Secrets / Variables

| Name | Type | Description |
|---|---|---|
| `FALCON_CLIENT_ID` | Secret | CrowdStrike API client ID (Correlation Rules: read+write) |
| `FALCON_CLIENT_SECRET` | Secret | CrowdStrike API client secret |
| `FALCON_BASE_URL` | Secret | Base URL (e.g. `https://api.crowdstrike.com`). Omit for auto-detect. |
| `LOG_LEVEL` | Variable (optional) | Default log level. Defaults to `INFO`. |

---

## Local setup

```bash
export FALCON_CLIENT_ID="your-client-id"
export FALCON_CLIENT_SECRET="your-client-secret"
export FALCON_BASE_URL="your-base-url"   # optional
export LOG_LEVEL="INFO"                  # optional
```

---

## First run — bootstrap from the API

If the `rules/` directory is empty, the script automatically pulls all existing
rules from your CrowdStrike tenant and writes one file per rule:

```bash
python scripts/sync_detections.py
```

After this runs you will see files like:

```
rules/
  Kerberoasting_Detector.json
  BYOVD_-_EDR_Kill.json
  ...
```

Commit and push these files to put your existing rules under version control.

---

## Creating a new rule

Add a new `.json` file to `rules/` without an `id` field:

```json
{
  "name": "My New Detection Rule",
  "severity": 50,
  "customer_id": "<<YOUR_CID>>",
  "search": {
    "filter": "event_simpleName='ProcessRollup2'+FileName='mimikatz.exe'",
    "outcome": "detection",
    "lookback": "75m",
    "trigger_mode": "summary"
  },
  "operation": {
    "schedule": {
      "definition": "@every 1h"
    },
    "start_on": "2025-01-01T00:00:00Z"
  },
  "status": "active"
}
```

The filename should match the rule name with spaces replaced by underscores,
e.g. `My_New_Detection_Rule.json`.  Push to `main` and the
`Update Correlation Rules` workflow creates the rule in the console and writes
back the API-assigned `id` into the file automatically.

---

## Updating a rule

Edit the relevant `rules/<rule_name>.json` file directly.  Push to `main`.
The workflow detects the diff, calls the update API for that rule, and
overwrites the file with the API-normalized state.

---

## Deleting a rule

Set `"deleted": true` in the rule's JSON file and push.  The workflow deletes
the rule from the console and then removes the local file.

```json
{
  "id": "abc123",
  "name": "Old Rule",
  "deleted": true,
  ...
}
```

---

## Workflows

### `update_correlation_rules.yml`

Triggers automatically on any push to `main` that touches a file matching
`rules/**.json`.  Also supports manual dispatch.

Flow:
1. Check out repo (full history for rebase)
2. Run `sync_detections.py` — creates/updates/deletes rules in the console
3. Commit updated rule files (API writes back `id` and normalized fields)
4. `git pull --rebase origin main` to handle concurrent runs
5. Push

### `on_demand_sync.yml`

Manual trigger only.  Use this when the console is the source of truth and
you want the repo to catch up — e.g. after making bulk changes in the UI.

Flow:
1. Pull all rules from the API
2. Write/update/remove individual rule files
3. Show a file-level diff summary in the workflow run summary
4. Commit and push only if changes were detected

---

## How `[skip ci]` prevents loops

Both workflows append `[skip ci]` to their commit messages.  This prevents
the commit the workflow pushes (the sync write-back) from triggering another
workflow run.

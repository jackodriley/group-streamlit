# Corporate Email Domains Analysis

This repository contains a Streamlit app for analysing Piano user exports and identifying likely corporate email domains.

## Files

- `streamlit_app.py` - Streamlit app for upload, filtering, ranking, and download
- `config/generic_email_domains.txt` - Editable list of generic consumer domains to exclude
- `requirements.txt` - Python dependencies

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Behaviour

- Upload a CSV or Excel export from Piano
- Automatically classifies `Access Count > 1` as `Subscriber` and `<= 1` as `Registered User`
- Filters out generic domains from `config/generic_email_domains.txt`
- Ranks top corporate domains and exact email addresses
- Shows tables, charts, and drill-down rows for each segment
- Exports results as CSV or Excel

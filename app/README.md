# App Layer (Streamlit)

This app is documentary-only and sandbox-safe:
- no real access activation,
- no SQLite connections,
- no real data reads.

## Panels
- Schema Explorer
- Mapping Validator UI
- Dry-Run Convergence Dashboard

## Run locally
```bash
cd '/Users/miguelmiguel/CODEX/HREVN UNIFIED V1 SANDBOX'
streamlit run app/streamlit_app.py
```

## Deploy to Streamlit Cloud
- Main file path: `app/streamlit_app.py`
- Python dependencies: `requirements.txt`
- Secret template: `.streamlit/secrets.toml.example`

# Webscrapper_basic

Simple config-driven domain crawler and file downloader.

## Quickstart

```bash
pip install -r requirements.txt
cp config.example.yml config.yml
python -m crawler discover --config config.yml
python -m crawler download --config config.yml
```

`discover` crawls pages within the configured domain, records downloadable files to `state/manifest.jsonl`, and prints a summary.
`download` retrieves any files listed in the manifest and saves them under `out/`,
replicating the site's domain and path. Unchanged files are skipped on re-runs.

## Incremental behaviour

The crawler keeps state in the `state/` directory:

- `visited_urls.txt`: pages already visited
- `manifest.jsonl`: metadata for each discovered or downloaded file

When `download` runs, it checks ETag/Last-Modified headers (or file hashes) to skip files that have not changed.

## Dashboard

Visualize the manifest with a simple Streamlit dashboard:

```bash
streamlit run dashboard.py
```

A config file path may be provided to choose a different state directory:

```bash
streamlit run dashboard.py -- --config config.yml
```

## Smoke test

Run the included script for a tiny end-to-end test against a public site:

```bash
bash scripts/smoke_run.sh
```

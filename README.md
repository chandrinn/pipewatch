# pipewatch

A lightweight CLI tool to monitor and visualize ETL pipeline health with configurable alerting thresholds.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/youruser/pipewatch.git && cd pipewatch && pip install .
```

---

## Usage

Run a health check against your pipeline configuration:

```bash
pipewatch monitor --config pipeline.yaml
```

Example `pipeline.yaml`:

```yaml
pipelines:
  - name: daily_sales_etl
    source: postgres://localhost/sales
    thresholds:
      max_latency_minutes: 30
      min_rows_processed: 1000
      error_rate_percent: 2.5
```

Watch mode with live dashboard:

```bash
pipewatch monitor --config pipeline.yaml --watch --interval 60
```

Send alerts when thresholds are breached:

```bash
pipewatch monitor --config pipeline.yaml --alert slack --webhook-url $SLACK_WEBHOOK
```

View pipeline status summary:

```bash
pipewatch status --last 24h
```

---

## Options

| Flag | Description |
|------|-------------|
| `--config` | Path to pipeline config file |
| `--watch` | Enable live refresh mode |
| `--interval` | Refresh interval in seconds (default: 30) |
| `--alert` | Alert channel (`slack`, `email`, `pagerduty`) |
| `--last` | Show history for a given time window |

---

## License

MIT © 2024 pipewatch contributors
# Proactive Network Fault Detection — Sri Lanka Telecom Demo

End-to-end demo showing how Databricks can transform telecom NOC operations from reactive to proactive using ML-powered anomaly detection and alarm correlation.

## What's Included

| File | Description |
|------|-------------|
| `notebooks/00_setup.py` | Creates Unity Catalog schema and 10 Delta tables (Bronze/Silver/Gold) |
| `notebooks/01_data_generation.py` | Generates synthetic SLT network data: 443 elements, 5 regions, 18 fault scenarios |
| `notebooks/02_medallion_pipeline.py` | Bronze → Silver → Gold pipeline with rolling stats, z-scores, health scores |
| `notebooks/03_anomaly_detection.py` | Isolation Forest + Gradient Boosted fault classifier, logged to MLflow |
| `notebooks/04_alarm_correlation.py` | Temporal + topological alarm correlation (7.5:1 noise reduction) |
| `notebooks/05_demo_script.py` | Presenter notebook with talking points, live queries, and Q&A guidance |
| `dashboard.json` | AI/BI Lakeview dashboard definition (3 pages) |
| `DEMO_SCRIPT.md` | Full demo script with customer context and objection handling |

## Setup Instructions

### 1. Configure Your Catalog

Every notebook uses a `CATALOG` variable at the top. Before running, update it to your catalog:

```python
CATALOG = "your_catalog_name"  # ← Change this in each notebook
```

The `dashboard.json` also references `YOUR_CATALOG` — do a find-and-replace before deploying.

### 2. Run Notebooks in Order

```
00_setup → 01_data_generation → 02_medallion_pipeline → 03_anomaly_detection → 04_alarm_correlation
```

Each notebook is self-contained. Estimated run times on serverless:
- 00_setup: ~1 min
- 01_data_generation: ~10-15 min
- 02_medallion_pipeline: ~10 min
- 03_anomaly_detection: ~5 min
- 04_alarm_correlation: ~3 min

### 3. Deploy the Dashboard

Update `YOUR_CATALOG` references in `dashboard.json`, then deploy via the Databricks CLI or UI.

### 4. Present

Open `05_demo_script` in your workspace — it contains the full presenter flow with live query cells.

## Key Demo Metrics

- **443** network elements across 5 Sri Lankan regions
- **18** injected fault scenarios (fiber cuts, cell overload, transceiver degradation, etc.)
- **7.5:1** alarm reduction through ML correlation
- **~55%** projected MTTR improvement with proactive detection
- **Unsupervised ML** — no labeled data required to get started

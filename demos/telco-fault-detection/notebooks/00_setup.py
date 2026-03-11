# Databricks notebook source
# MAGIC %md
# MAGIC # Proactive Network Fault Detection — Environment Setup
# MAGIC ### Sri Lanka Telecom Demo
# MAGIC
# MAGIC This notebook creates the Unity Catalog schema and tables for the Proactive Network Fault Detection demo.
# MAGIC
# MAGIC **Architecture:** Medallion (Bronze → Silver → Gold) with ML-powered anomaly detection and alarm correlation.

# COMMAND ----------

CATALOG = "serverless_stable_lyrggh_catalog"
SCHEMA = "telco_network_fault_detection"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"✓ Using {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables — Raw Ingested Data

# COMMAND ----------

# Network Topology — master reference of all network elements
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_network_topology (
  element_id STRING COMMENT 'Unique network element identifier (e.g., COL-ENB-001)',
  element_type STRING COMMENT 'Type: eNodeB, Router, Switch, FiberLink, CoreNode',
  element_name STRING COMMENT 'Human-readable name',
  region STRING COMMENT 'Geographic region',
  city STRING COMMENT 'City name',
  latitude DOUBLE,
  longitude DOUBLE,
  parent_element_id STRING COMMENT 'Upstream element this connects to',
  vendor STRING COMMENT 'Equipment vendor',
  model STRING COMMENT 'Equipment model',
  install_date DATE,
  status STRING COMMENT 'Active, Maintenance, Decommissioned',
  capacity_gbps DOUBLE COMMENT 'Maximum capacity in Gbps',
  created_at TIMESTAMP
) USING DELTA
COMMENT 'Raw network topology and inventory data'
""")

# COMMAND ----------

# Performance Metrics (PM Counters) — time-series KPIs from network elements
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_pm_counters (
  element_id STRING,
  timestamp TIMESTAMP COMMENT '5-minute interval timestamp',
  metric_name STRING COMMENT 'KPI name (e.g., cpu_pct, latency_ms, throughput_dl)',
  metric_value DOUBLE,
  unit STRING,
  collection_type STRING COMMENT 'GAUGE or COUNTER',
  ingested_at TIMESTAMP
) USING DELTA
COMMENT 'Raw performance counter readings from network elements at 5-min intervals'
PARTITIONED BY (element_id)
""")

# COMMAND ----------

# Alarms (Fault Management)
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_alarms (
  alarm_id STRING,
  element_id STRING,
  alarm_time TIMESTAMP,
  clear_time TIMESTAMP,
  severity STRING COMMENT 'CRITICAL, MAJOR, MINOR, WARNING',
  alarm_type STRING COMMENT 'e.g., LINK_DOWN, HIGH_BER, CPU_OVERLOAD',
  alarm_text STRING COMMENT 'Detailed alarm description',
  probable_cause STRING,
  is_active BOOLEAN,
  acknowledged BOOLEAN,
  acknowledged_by STRING,
  ingested_at TIMESTAMP
) USING DELTA
COMMENT 'Raw fault management alarms from network elements'
""")

# COMMAND ----------

# Trouble Tickets — historical incidents
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_trouble_tickets (
  ticket_id STRING,
  created_at TIMESTAMP,
  resolved_at TIMESTAMP,
  priority STRING COMMENT 'P1, P2, P3, P4',
  category STRING COMMENT 'Hardware, Software, Fiber, Power, Capacity',
  root_cause STRING COMMENT 'Identified root cause',
  affected_elements STRING COMMENT 'Comma-separated element IDs',
  affected_region STRING,
  affected_customers INT COMMENT 'Estimated customer impact',
  resolution_notes STRING,
  mttr_minutes INT COMMENT 'Mean time to repair in minutes'
) USING DELTA
COMMENT 'Historical trouble tickets with root cause labels'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables — Cleaned, Enriched, Aggregated

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_pm_enriched (
  element_id STRING,
  element_type STRING,
  element_name STRING,
  region STRING,
  city STRING,
  timestamp TIMESTAMP,
  metric_name STRING,
  metric_value DOUBLE,
  unit STRING,
  -- Rolling aggregates
  rolling_avg_1h DOUBLE COMMENT '1-hour rolling average',
  rolling_std_1h DOUBLE COMMENT '1-hour rolling standard deviation',
  rolling_avg_6h DOUBLE COMMENT '6-hour rolling average',
  rolling_avg_24h DOUBLE COMMENT '24-hour rolling average',
  -- Threshold flags
  zscore DOUBLE COMMENT 'Z-score relative to 24h rolling stats',
  threshold_violated BOOLEAN COMMENT 'Static threshold violation flag',
  hour_of_day INT,
  day_of_week INT
) USING DELTA
COMMENT 'Enriched PM counters with topology join and rolling aggregates'
PARTITIONED BY (element_type)
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_alarms_enriched (
  alarm_id STRING,
  element_id STRING,
  element_type STRING,
  element_name STRING,
  region STRING,
  city STRING,
  alarm_time TIMESTAMP,
  clear_time TIMESTAMP,
  duration_minutes DOUBLE,
  severity STRING,
  severity_score INT COMMENT 'Numeric: CRITICAL=4, MAJOR=3, MINOR=2, WARNING=1',
  alarm_type STRING,
  alarm_text STRING,
  probable_cause STRING,
  parent_element_id STRING,
  is_active BOOLEAN
) USING DELTA
COMMENT 'Enriched alarms joined with topology data'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Tables — Analytics & ML Ready

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS gold_network_health (
  timestamp TIMESTAMP COMMENT 'Hourly aggregation timestamp',
  region STRING,
  element_type STRING,
  total_elements INT,
  healthy_elements INT,
  degraded_elements INT,
  critical_elements INT,
  health_score DOUBLE COMMENT '0-100 health score',
  avg_cpu_pct DOUBLE,
  avg_latency_ms DOUBLE,
  avg_packet_loss_pct DOUBLE,
  total_alarms INT,
  critical_alarms INT,
  anomalies_detected INT
) USING DELTA
COMMENT 'Hourly network health scores by region and element type'
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS gold_anomaly_scores (
  element_id STRING,
  element_type STRING,
  region STRING,
  timestamp TIMESTAMP,
  anomaly_score DOUBLE COMMENT 'Isolation Forest anomaly score (-1 to 0, lower = more anomalous)',
  is_anomaly BOOLEAN,
  contributing_metrics STRING COMMENT 'JSON: metrics that drove the anomaly',
  severity STRING COMMENT 'Derived severity from score: HIGH, MEDIUM, LOW',
  model_version STRING
) USING DELTA
COMMENT 'ML anomaly detection results per element per time window'
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS gold_correlated_incidents (
  incident_id STRING,
  detected_at TIMESTAMP,
  root_cause_element STRING COMMENT 'Probable root cause network element',
  root_cause_type STRING COMMENT 'Probable fault type',
  affected_elements STRING COMMENT 'JSON array of impacted downstream elements',
  affected_count INT,
  raw_alarm_count INT COMMENT 'Number of raw alarms correlated into this incident',
  max_severity STRING,
  region STRING,
  estimated_customer_impact INT,
  status STRING COMMENT 'OPEN, INVESTIGATING, RESOLVED'
) USING DELTA
COMMENT 'Correlated incidents from alarm grouping and root cause analysis'
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS gold_ml_features (
  element_id STRING,
  element_type STRING,
  region STRING,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  -- Aggregated features
  avg_cpu_pct DOUBLE,
  max_cpu_pct DOUBLE,
  std_cpu_pct DOUBLE,
  avg_memory_pct DOUBLE,
  avg_latency_ms DOUBLE,
  max_latency_ms DOUBLE,
  p95_latency_ms DOUBLE,
  avg_packet_loss_pct DOUBLE,
  max_packet_loss_pct DOUBLE,
  avg_bandwidth_util DOUBLE,
  avg_error_count DOUBLE,
  total_alarms_1h INT,
  critical_alarms_1h INT,
  avg_throughput_dl DOUBLE,
  avg_throughput_ul DOUBLE,
  handover_success_rate DOUBLE,
  call_drop_rate DOUBLE,
  hour_of_day INT,
  day_of_week INT,
  -- Label (from trouble tickets, NULL if no incident)
  has_fault BOOLEAN,
  fault_type STRING
) USING DELTA
COMMENT 'Feature table for ML model training and scoring'
""")

# COMMAND ----------

# Verify all tables created
tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
print(f"✓ Created {len(tables)} tables in {CATALOG}.{SCHEMA}:")
for t in tables:
    print(f"  - {t.tableName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Run `01_data_generation` to populate with synthetic Sri Lanka Telecom data
# MAGIC 2. Run `02_medallion_pipeline` to process Bronze → Silver → Gold
# MAGIC 3. Run `03_anomaly_detection` to train and score ML models
# MAGIC 4. Run `04_alarm_correlation` to correlate alarms into incidents

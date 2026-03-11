# Databricks notebook source
# MAGIC %md
# MAGIC # Proactive Network Fault Detection — Medallion Pipeline
# MAGIC ### Sri Lanka Telecom Demo
# MAGIC
# MAGIC Processes raw Bronze data through Silver (cleaned, enriched) to Gold (analytics-ready) layers.
# MAGIC
# MAGIC **Silver Layer:**
# MAGIC - Join PM counters with topology
# MAGIC - Compute rolling aggregates (1h, 6h, 24h)
# MAGIC - Calculate z-scores for anomaly flagging
# MAGIC - Enrich alarms with topology data
# MAGIC
# MAGIC **Gold Layer:**
# MAGIC - Network health scores by region
# MAGIC - ML feature table
# MAGIC - Alarm correlation preparation

# COMMAND ----------

CATALOG = "YOUR_CATALOG"
SCHEMA = "telco_network_fault_detection"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Enriched PM Counters
# MAGIC
# MAGIC Join performance metrics with topology to add element context (type, region, city),
# MAGIC then compute rolling statistics for anomaly detection.

# COMMAND ----------

# Load bronze data
pm_df = spark.table("bronze_pm_counters")
topology_df = spark.table("bronze_network_topology").select(
    "element_id", "element_type", "element_name", "region", "city", "parent_element_id"
)

print(f"PM counters: {pm_df.count():,} rows")
print(f"Topology: {topology_df.count()} elements")

# COMMAND ----------

# Join PM with topology
pm_enriched = pm_df.join(topology_df, on="element_id", how="inner")

# Add time features
pm_enriched = pm_enriched \
    .withColumn("hour_of_day", F.hour("timestamp")) \
    .withColumn("day_of_week", F.dayofweek("timestamp"))

print(f"Enriched PM: {pm_enriched.count():,} rows")

# COMMAND ----------

# Compute rolling aggregates using window functions
# Window: per element + metric, ordered by time

window_1h = Window.partitionBy("element_id", "metric_name") \
    .orderBy(F.col("timestamp").cast("long")) \
    .rangeBetween(-3600, 0)  # 1 hour lookback

window_6h = Window.partitionBy("element_id", "metric_name") \
    .orderBy(F.col("timestamp").cast("long")) \
    .rangeBetween(-21600, 0)  # 6 hours

window_24h = Window.partitionBy("element_id", "metric_name") \
    .orderBy(F.col("timestamp").cast("long")) \
    .rangeBetween(-86400, 0)  # 24 hours

silver_pm = pm_enriched \
    .withColumn("rolling_avg_1h", F.avg("metric_value").over(window_1h)) \
    .withColumn("rolling_std_1h", F.stddev("metric_value").over(window_1h)) \
    .withColumn("rolling_avg_6h", F.avg("metric_value").over(window_6h)) \
    .withColumn("rolling_avg_24h", F.avg("metric_value").over(window_24h))

# Z-score: how many standard deviations from 24h rolling mean
window_24h_stats = Window.partitionBy("element_id", "metric_name") \
    .orderBy(F.col("timestamp").cast("long")) \
    .rangeBetween(-86400, 0)

silver_pm = silver_pm \
    .withColumn("_rolling_std_24h", F.stddev("metric_value").over(window_24h_stats)) \
    .withColumn("zscore",
        F.when(F.col("_rolling_std_24h") > 0,
               (F.col("metric_value") - F.col("rolling_avg_24h")) / F.col("_rolling_std_24h"))
        .otherwise(0.0)) \
    .drop("_rolling_std_24h")

# Static threshold violations (domain-specific thresholds)
silver_pm = silver_pm.withColumn("threshold_violated",
    F.when((F.col("metric_name") == "cpu_pct") & (F.col("metric_value") > 85), True)
    .when((F.col("metric_name") == "memory_pct") & (F.col("metric_value") > 90), True)
    .when((F.col("metric_name") == "packet_loss_pct") & (F.col("metric_value") > 1.0), True)
    .when((F.col("metric_name") == "latency_ms") & (F.col("metric_value") > 50), True)
    .when((F.col("metric_name") == "temperature_c") & (F.col("metric_value") > 65), True)
    .when((F.col("metric_name") == "call_drop_rate") & (F.col("metric_value") > 3.0), True)
    .when((F.col("metric_name") == "handover_success_rate") & (F.col("metric_value") < 90), True)
    .when((F.col("metric_name") == "optical_power_dbm") & (F.col("metric_value") < -20), True)
    .when((F.col("metric_name") == "bandwidth_utilization_pct") & (F.col("metric_value") > 85), True)
    .when((F.col("metric_name") == "ber") & (F.col("metric_value") > 1e-6), True)
    .when(F.abs(F.col("zscore")) > 3.0, True)  # Z-score based threshold
    .otherwise(False)
)

# Select final columns
silver_pm_final = silver_pm.select(
    "element_id", "element_type", "element_name", "region", "city",
    "timestamp", "metric_name", "metric_value", "unit",
    "rolling_avg_1h", "rolling_std_1h", "rolling_avg_6h", "rolling_avg_24h",
    "zscore", "threshold_violated", "hour_of_day", "day_of_week"
)

silver_pm_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_pm_enriched")
print(f"✓ Wrote {silver_pm_final.count():,} rows to silver_pm_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Enriched Alarms

# COMMAND ----------

alarms_df = spark.table("bronze_alarms")

# Join with topology
silver_alarms = alarms_df.join(
    topology_df, on="element_id", how="inner"
)

# Calculate duration and severity score
silver_alarms = silver_alarms \
    .withColumn("duration_minutes",
        (F.unix_timestamp("clear_time") - F.unix_timestamp("alarm_time")) / 60.0) \
    .withColumn("severity_score",
        F.when(F.col("severity") == "CRITICAL", 4)
        .when(F.col("severity") == "MAJOR", 3)
        .when(F.col("severity") == "MINOR", 2)
        .when(F.col("severity") == "WARNING", 1)
        .otherwise(0))

silver_alarms_final = silver_alarms.select(
    "alarm_id", "element_id", "element_type", "element_name",
    "region", "city", "alarm_time", "clear_time", "duration_minutes",
    "severity", "severity_score", "alarm_type", "alarm_text",
    "probable_cause", "parent_element_id", "is_active"
)

silver_alarms_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_alarms_enriched")
print(f"✓ Wrote {silver_alarms_final.count():,} rows to silver_alarms_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Network Health Scores
# MAGIC
# MAGIC Hourly health score per region and element type, computed from PM thresholds and alarm counts.

# COMMAND ----------

# Hourly aggregated PM stats
pm_hourly = spark.table("silver_pm_enriched") \
    .withColumn("hour_ts", F.date_trunc("hour", "timestamp"))

# Count threshold violations per element per hour
violations_per_element = pm_hourly.filter(F.col("threshold_violated") == True) \
    .groupBy("hour_ts", "element_id", "element_type", "region") \
    .agg(F.count("*").alias("violation_count"))

# Get all elements active per hour
all_elements_hourly = pm_hourly \
    .select("hour_ts", "element_id", "element_type", "region").distinct()

# Classify element health: 0 violations = healthy, 1-3 = degraded, 4+ = critical
element_health = all_elements_hourly.join(
    violations_per_element,
    on=["hour_ts", "element_id", "element_type", "region"],
    how="left"
).fillna(0, subset=["violation_count"])

element_health = element_health.withColumn("health_status",
    F.when(F.col("violation_count") == 0, "healthy")
    .when(F.col("violation_count") <= 3, "degraded")
    .otherwise("critical"))

# Aggregate to region + element_type level
health_agg = element_health.groupBy("hour_ts", "region", "element_type").agg(
    F.count("*").alias("total_elements"),
    F.sum(F.when(F.col("health_status") == "healthy", 1).otherwise(0)).alias("healthy_elements"),
    F.sum(F.when(F.col("health_status") == "degraded", 1).otherwise(0)).alias("degraded_elements"),
    F.sum(F.when(F.col("health_status") == "critical", 1).otherwise(0)).alias("critical_elements"),
)

# Health score = (healthy * 100 + degraded * 50 + critical * 0) / total
health_agg = health_agg.withColumn("health_score",
    (F.col("healthy_elements") * 100 + F.col("degraded_elements") * 50) / F.col("total_elements")
)

# Add average KPIs
avg_kpis = pm_hourly.groupBy("hour_ts", "region", "element_type").agg(
    F.avg(F.when(F.col("metric_name") == "cpu_pct", F.col("metric_value"))).alias("avg_cpu_pct"),
    F.avg(F.when(F.col("metric_name") == "latency_ms", F.col("metric_value"))).alias("avg_latency_ms"),
    F.avg(F.when(F.col("metric_name") == "packet_loss_pct", F.col("metric_value"))).alias("avg_packet_loss_pct"),
)

# Alarm counts per hour/region
alarm_counts = spark.table("silver_alarms_enriched") \
    .withColumn("hour_ts", F.date_trunc("hour", "alarm_time")) \
    .groupBy("hour_ts", "region", "element_type").agg(
        F.count("*").alias("total_alarms"),
        F.sum(F.when(F.col("severity") == "CRITICAL", 1).otherwise(0)).alias("critical_alarms"),
    )

# Anomaly count placeholder (will be populated by ML notebook)
gold_health = health_agg \
    .join(avg_kpis, on=["hour_ts", "region", "element_type"], how="left") \
    .join(alarm_counts, on=["hour_ts", "region", "element_type"], how="left") \
    .fillna(0, subset=["total_alarms", "critical_alarms"]) \
    .withColumn("anomalies_detected", F.lit(0)) \
    .withColumnRenamed("hour_ts", "timestamp")

gold_health.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_network_health")
print(f"✓ Wrote {gold_health.count():,} rows to gold_network_health")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: ML Feature Table
# MAGIC
# MAGIC Pivot PM counters into a wide feature table for ML model training.
# MAGIC Each row = one element + one 1-hour window with all metrics as columns.

# COMMAND ----------

# Pivot metrics into columns — 1-hour windows
pm_for_features = spark.table("silver_pm_enriched") \
    .withColumn("window_start", F.date_trunc("hour", "timestamp"))

# Aggregate metrics per element per hour
feature_aggs = pm_for_features.groupBy("element_id", "element_type", "region", "window_start").agg(
    F.avg(F.when(F.col("metric_name") == "cpu_pct", F.col("metric_value"))).alias("avg_cpu_pct"),
    F.max(F.when(F.col("metric_name") == "cpu_pct", F.col("metric_value"))).alias("max_cpu_pct"),
    F.stddev(F.when(F.col("metric_name") == "cpu_pct", F.col("metric_value"))).alias("std_cpu_pct"),
    F.avg(F.when(F.col("metric_name") == "memory_pct", F.col("metric_value"))).alias("avg_memory_pct"),
    F.avg(F.when(F.col("metric_name") == "latency_ms", F.col("metric_value"))).alias("avg_latency_ms"),
    F.max(F.when(F.col("metric_name") == "latency_ms", F.col("metric_value"))).alias("max_latency_ms"),
    F.percentile_approx(F.when(F.col("metric_name") == "latency_ms", F.col("metric_value")), 0.95).alias("p95_latency_ms"),
    F.avg(F.when(F.col("metric_name") == "packet_loss_pct", F.col("metric_value"))).alias("avg_packet_loss_pct"),
    F.max(F.when(F.col("metric_name") == "packet_loss_pct", F.col("metric_value"))).alias("max_packet_loss_pct"),
    F.avg(F.when(F.col("metric_name") == "bandwidth_utilization_pct", F.col("metric_value"))).alias("avg_bandwidth_util"),
    F.avg(F.when(F.col("metric_name").isin("error_count", "interface_errors"), F.col("metric_value"))).alias("avg_error_count"),
    F.avg(F.when(F.col("metric_name") == "throughput_dl_mbps", F.col("metric_value"))).alias("avg_throughput_dl"),
    F.avg(F.when(F.col("metric_name") == "throughput_ul_mbps", F.col("metric_value"))).alias("avg_throughput_ul"),
    F.avg(F.when(F.col("metric_name") == "handover_success_rate", F.col("metric_value"))).alias("handover_success_rate"),
    F.avg(F.when(F.col("metric_name") == "call_drop_rate", F.col("metric_value"))).alias("call_drop_rate"),
)

# Add time features
feature_aggs = feature_aggs \
    .withColumn("window_end", F.col("window_start") + F.expr("INTERVAL 1 HOUR")) \
    .withColumn("hour_of_day", F.hour("window_start")) \
    .withColumn("day_of_week", F.dayofweek("window_start"))

# Add alarm counts per element per hour
alarm_features = spark.table("silver_alarms_enriched") \
    .withColumn("window_start", F.date_trunc("hour", "alarm_time")) \
    .groupBy("element_id", "window_start").agg(
        F.count("*").alias("total_alarms_1h"),
        F.sum(F.when(F.col("severity") == "CRITICAL", 1).otherwise(0)).alias("critical_alarms_1h"),
    )

feature_table = feature_aggs.join(
    alarm_features, on=["element_id", "window_start"], how="left"
).fillna(0, subset=["total_alarms_1h", "critical_alarms_1h"])

# Add fault labels from trouble tickets
tickets = spark.table("bronze_trouble_tickets")

# Explode affected elements and create time ranges for label join
from pyspark.sql.functions import explode, split

ticket_labels = tickets.select(
    explode(split("affected_elements", ",")).alias("element_id"),
    F.col("created_at").alias("fault_start"),
    F.col("resolved_at").alias("fault_end"),
    F.col("root_cause").alias("fault_type")
)

# Label: has_fault = True if element had a fault in this hour window
feature_table = feature_table.join(
    ticket_labels,
    (feature_table.element_id == ticket_labels.element_id) &
    (feature_table.window_start >= ticket_labels.fault_start) &
    (feature_table.window_start <= ticket_labels.fault_end),
    how="left"
).select(
    feature_table["*"],
    F.when(ticket_labels.fault_type.isNotNull(), True).otherwise(False).alias("has_fault"),
    ticket_labels.fault_type
)

feature_table.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_ml_features")
count = feature_table.count()
fault_count = feature_table.filter(F.col("has_fault") == True).count()
print(f"✓ Wrote {count:,} rows to gold_ml_features")
print(f"  Fault windows: {fault_count:,} ({fault_count/count*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

print("=" * 60)
print("MEDALLION PIPELINE COMPLETE")
print("=" * 60)
for table in ["silver_pm_enriched", "silver_alarms_enriched", "gold_network_health", "gold_ml_features"]:
    count = spark.table(table).count()
    print(f"  {table}: {count:,} rows")
print("=" * 60)
print("\nNext: Run 03_anomaly_detection to train ML models")

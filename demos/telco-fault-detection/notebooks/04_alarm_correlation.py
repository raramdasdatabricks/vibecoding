# Databricks notebook source
# MAGIC %md
# MAGIC # Proactive Network Fault Detection — Alarm Correlation
# MAGIC ### Sri Lanka Telecom Demo
# MAGIC
# MAGIC **Problem:** A single root cause (e.g., fiber cut) triggers dozens or hundreds of alarms
# MAGIC across dependent network elements. NOC operators get overwhelmed by alarm storms.
# MAGIC
# MAGIC **Solution:** Correlate alarms using:
# MAGIC 1. **Temporal proximity** — alarms within a time window likely share a cause
# MAGIC 2. **Topological dependency** — alarms on elements in the same dependency chain
# MAGIC 3. **Root cause inference** — identify the most upstream element as probable root cause
# MAGIC
# MAGIC **Target:** Reduce raw alarm volume by 10:1 or better, surfacing actionable incidents.

# COMMAND ----------

CATALOG = "YOUR_CATALOG"
SCHEMA = "telco_network_fault_detection"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Enriched Alarms and Topology

# COMMAND ----------

alarms = spark.table("silver_alarms_enriched")
topology = spark.table("bronze_network_topology")

print(f"Total alarms: {alarms.count():,}")
print(f"Severity distribution:")
alarms.groupBy("severity").count().orderBy("severity").show()

# COMMAND ----------

# Build the topology graph: element → parent chain
# This lets us determine if two alarming elements share an upstream dependency

topo_pd = topology.select("element_id", "parent_element_id", "element_type", "region").toPandas()

# Build parent chain for each element (up to core)
parent_chain = {}
for _, row in topo_pd.iterrows():
    chain = [row["element_id"]]
    current = row["element_id"]
    visited = set()
    while True:
        parent_row = topo_pd[topo_pd["element_id"] == current]
        if parent_row.empty:
            break
        parent = parent_row.iloc[0]["parent_element_id"]
        if parent is None or parent in visited:
            break
        chain.append(parent)
        visited.add(parent)
        current = parent
    parent_chain[row["element_id"]] = chain

# Helper: find common ancestor of two elements
def common_ancestor(eid1, eid2):
    chain1 = set(parent_chain.get(eid1, [eid1]))
    for ancestor in parent_chain.get(eid2, [eid2]):
        if ancestor in chain1:
            return ancestor
    return None

# Helper: find the most upstream element in a group
def find_root_cause(element_ids):
    """The root cause is the element whose parent chain is shortest (most upstream)."""
    if not element_ids:
        return None
    min_depth = float("inf")
    root = element_ids[0]
    for eid in element_ids:
        depth = len(parent_chain.get(eid, [eid]))
        if depth < min_depth:
            min_depth = depth
            root = eid
    return root

print(f"Built parent chains for {len(parent_chain)} elements")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Temporal + Topological Alarm Correlation
# MAGIC
# MAGIC Algorithm:
# MAGIC 1. Sort alarms by time
# MAGIC 2. For each alarm, check if it can be merged into an existing open correlation group:
# MAGIC    - Within 15-minute window of the group's first alarm
# MAGIC    - Shares a common ancestor with at least one element in the group
# MAGIC 3. If no match, start a new correlation group
# MAGIC 4. For each group, identify the most upstream element as root cause

# COMMAND ----------

# Pull alarms to pandas for graph-based correlation
alarms_pd = alarms.select(
    "alarm_id", "element_id", "element_type", "element_name",
    "region", "alarm_time", "severity", "severity_score",
    "alarm_type", "probable_cause", "parent_element_id"
).orderBy("alarm_time").toPandas()

CORRELATION_WINDOW_MINUTES = 15

# Correlation algorithm
groups = []  # List of {"start_time", "elements", "alarms", "region"}
alarm_to_group = {}

for idx, alarm in alarms_pd.iterrows():
    eid = alarm["element_id"]
    atime = alarm["alarm_time"]
    region = alarm["region"]

    matched_group = None

    # Try to match to existing open groups
    for g in groups:
        # Check time window
        if (atime - g["start_time"]).total_seconds() > CORRELATION_WINDOW_MINUTES * 60:
            continue
        # Check same region
        if g["region"] != region:
            continue
        # Check topological relationship
        for existing_eid in g["elements"]:
            ancestor = common_ancestor(eid, existing_eid)
            if ancestor is not None:
                matched_group = g
                break
        if matched_group:
            break

    if matched_group:
        matched_group["elements"].add(eid)
        matched_group["alarms"].append(alarm.to_dict())
        matched_group["max_severity"] = max(matched_group["max_severity"], alarm["severity_score"])
    else:
        groups.append({
            "start_time": atime,
            "elements": {eid},
            "alarms": [alarm.to_dict()],
            "region": region,
            "max_severity": alarm["severity_score"]
        })

print(f"Correlation complete:")
print(f"  Raw alarms: {len(alarms_pd):,}")
print(f"  Correlated groups: {len(groups):,}")
print(f"  Reduction ratio: {len(alarms_pd)/max(len(groups),1):.1f}:1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build Correlated Incident Records

# COMMAND ----------

import json

severity_map = {4: "CRITICAL", 3: "MAJOR", 2: "MINOR", 1: "WARNING"}

# Estimate customer impact per element type
customers_per_type = {"eNodeB": 200, "Router": 5000, "Switch": 2000, "FiberLink": 3000, "CoreNode": 50000}

incidents = []
for i, group in enumerate(groups):
    if len(group["alarms"]) < 2 and group["max_severity"] < 3:
        continue  # Skip isolated minor alarms

    element_list = list(group["elements"])
    root_cause_element = find_root_cause(element_list)

    # Determine root cause type from the root element's alarm types
    root_alarms = [a for a in group["alarms"] if a["element_id"] == root_cause_element]
    root_cause_type = root_alarms[0]["probable_cause"] if root_alarms else group["alarms"][0]["probable_cause"]

    # Estimate customer impact
    impact = 0
    for eid in element_list:
        etype_rows = topo_pd[topo_pd["element_id"] == eid]
        if not etype_rows.empty:
            etype = etype_rows.iloc[0]["element_type"]
            impact += customers_per_type.get(etype, 500)

    incidents.append({
        "incident_id": f"INC-{i+1:04d}",
        "detected_at": str(group["start_time"]),
        "root_cause_element": root_cause_element,
        "root_cause_type": root_cause_type,
        "affected_elements": json.dumps(element_list[:20]),
        "affected_count": len(element_list),
        "raw_alarm_count": len(group["alarms"]),
        "max_severity": severity_map.get(group["max_severity"], "MINOR"),
        "region": group["region"],
        "estimated_customer_impact": min(impact, 100000),
        "status": "RESOLVED"
    })

print(f"Generated {len(incidents)} correlated incidents")
print(f"\nTop incidents by alarm count:")
for inc in sorted(incidents, key=lambda x: x["raw_alarm_count"], reverse=True)[:10]:
    print(f"  {inc['incident_id']}: {inc['raw_alarm_count']:3d} alarms → 1 incident | "
          f"{inc['root_cause_type']:30s} | {inc['region']:10s} | {inc['max_severity']}")

# COMMAND ----------

# Write to gold table
from pyspark.sql import Row

incident_rows = [Row(**inc) for inc in incidents]
incident_df = spark.createDataFrame(incident_rows)
incident_df = incident_df.withColumn("detected_at", F.to_timestamp("detected_at"))

incident_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_correlated_incidents")
print(f"✓ Wrote {incident_df.count()} correlated incidents to gold_correlated_incidents")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Correlation Effectiveness Analysis

# COMMAND ----------

# Show the power of correlation: alarm storms reduced to single incidents
print("=" * 70)
print("ALARM CORRELATION EFFECTIVENESS")
print("=" * 70)

total_raw = len(alarms_pd)
total_incidents = len(incidents)
total_incident_alarms = sum(inc["raw_alarm_count"] for inc in incidents)

print(f"\n  Total raw alarms:        {total_raw:,}")
print(f"  Correlated into:         {total_incidents:,} incidents")
print(f"  Reduction ratio:         {total_raw/max(total_incidents,1):.1f}:1")
print(f"  Alarms in incidents:     {total_incident_alarms:,}")
print(f"  Standalone (noise):      {total_raw - total_incident_alarms:,}")

print(f"\n  By severity:")
for sev in ["CRITICAL", "MAJOR", "MINOR", "WARNING"]:
    count = len([i for i in incidents if i["max_severity"] == sev])
    print(f"    {sev:10s}: {count}")

print(f"\n  By region:")
for region in sorted(set(i["region"] for i in incidents)):
    count = len([i for i in incidents if i["region"] == region])
    alarms_in_region = sum(i["raw_alarm_count"] for i in incidents if i["region"] == region)
    print(f"    {region:10s}: {count:3d} incidents from {alarms_in_region:,} alarms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. MTTR Improvement Projection
# MAGIC
# MAGIC With proactive detection and alarm correlation:
# MAGIC - **Reactive MTTR**: Time from alarm → ticket → investigation → fix
# MAGIC - **Proactive MTTR**: ML detects anomaly early → pre-staged response → faster fix

# COMMAND ----------

tickets = spark.table("bronze_trouble_tickets").toPandas()

print("MTTR Comparison: Reactive vs Proactive")
print("=" * 70)
print(f"{'Root Cause':<30s} {'Reactive MTTR':>15s} {'Est. Proactive':>15s} {'Improvement':>15s}")
print("-" * 70)

total_reactive = 0
total_proactive = 0

for _, ticket in tickets.iterrows():
    reactive_mttr = ticket["mttr_minutes"]
    # Proactive reduction estimates based on fault type
    reduction = {
        "FIBER_CUT": 0.4,           # Pre-position splice teams
        "TRANSCEIVER_DEGRADATION": 0.7,  # Replace before failure
        "CELL_OVERLOAD": 0.6,       # Pre-provision capacity
        "CPU_OVERLOAD": 0.5,        # Auto-remediation scripts
        "MEMORY_LEAK": 0.8,         # Scheduled restart before OOM
        "POWER_SUPPLY_FAILURE": 0.3, # Hard to predict, but pre-stage spares
        "TEMPERATURE_HIGH": 0.6,     # Early HVAC dispatch
        "LINK_FLAP": 0.5,           # Identify flapping before outage
        "CORE_CONGESTION": 0.5,     # Auto-scale / traffic engineering
        "RADIO_DEGRADATION": 0.7,   # Correct before QoE impact
        "HANDOVER_FAILURE": 0.6,    # Rollback before widespread impact
    }
    pct = reduction.get(ticket["root_cause"], 0.4)
    proactive_mttr = int(reactive_mttr * (1 - pct))

    total_reactive += reactive_mttr
    total_proactive += proactive_mttr

    print(f"  {ticket['root_cause']:<30s} {reactive_mttr:>12d}m {proactive_mttr:>12d}m {pct*100:>12.0f}%")

print("-" * 70)
print(f"  {'TOTAL':<30s} {total_reactive:>12d}m {total_proactive:>12d}m {(1-total_proactive/total_reactive)*100:>12.0f}%")
print(f"\n  Overall MTTR reduction: {(1-total_proactive/total_reactive)*100:.0f}%")
print(f"  Equivalent to saving {(total_reactive-total_proactive)/60:.0f} engineer-hours per month")

# COMMAND ----------

print("✓ Alarm Correlation complete")
print("  Correlated incidents written to gold_correlated_incidents")
print("\nDemo is ready! Review the AI/BI dashboard for the visual overview.")

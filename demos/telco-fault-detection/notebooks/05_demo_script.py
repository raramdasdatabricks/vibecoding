# Databricks notebook source
# MAGIC %md
# MAGIC # Proactive Network Fault Detection — Demo Script
# MAGIC ## Sri Lanka Telecom (SLT-Mobitel)
# MAGIC
# MAGIC **Duration:** 45-60 minutes
# MAGIC **Audience:** SLT Network Operations, CTO Office, Digital Transformation Team
# MAGIC **Presenter:** [Your Name], Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Demo Setup (10 min before)
# MAGIC
# MAGIC 1. Open these tabs in your browser:
# MAGIC    - **Dashboard**: Navigate to `Dashboards` → "Sri Lanka Telecom - Network Fault Detection"
# MAGIC    - **This Notebook** (Demo Script — you're here)
# MAGIC    - **Notebook 01** (Data Generation): `telco_network_fault_detection/01_data_generation`
# MAGIC    - **Notebook 03** (Anomaly Detection): `telco_network_fault_detection/03_anomaly_detection`
# MAGIC    - **Notebook 04** (Alarm Correlation): `telco_network_fault_detection/04_alarm_correlation`
# MAGIC    - **MLflow Experiment**: `telco_fault_detection_experiment`
# MAGIC 2. Ensure the dashboard is loaded and showing data
# MAGIC 3. Have the architecture slide ready (or sketch on whiteboard)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 1: Setting the Context (5 min)
# MAGIC
# MAGIC ## Opening — Make It About Them
# MAGIC
# MAGIC > *"SLT-Mobitel operates one of the most critical infrastructure networks in Sri Lanka — 45,000 km of fiber, 7 million mobile subscribers, and you've just launched 5G in Colombo. Your network is the backbone of Sri Lanka's digital transformation."*
# MAGIC >
# MAGIC > *"But with this growth comes complexity. After Cyclone Ditwah last year, we saw how climate events can cascade through the network — fiber cuts, tower outages, power failures hitting multiple regions simultaneously. Your NOC teams dealt with thousands of alarms in hours."*
# MAGIC >
# MAGIC > *"Today I want to show you how Databricks can help SLT move from **reactive** firefighting to **proactive** fault detection — catching problems before your customers even notice."*
# MAGIC
# MAGIC ## The Problem Statement
# MAGIC
# MAGIC | Today (Reactive) | Tomorrow (Proactive) |
# MAGIC |---|---|
# MAGIC | Alarm fires → NOC investigates | ML detects anomaly → Pre-staged response |
# MAGIC | 1000s of alarms in a storm → Alert fatigue | Smart correlation → 1 actionable incident |
# MAGIC | Static thresholds miss gradual degradation | ML catches transceiver aging weeks early |
# MAGIC | MTTR: hours | MTTR: minutes |
# MAGIC | Customers call you about outages | You fix it before customers notice |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 2: Architecture Walkthrough (5 min)
# MAGIC
# MAGIC > *"The solution runs entirely on the Databricks Lakehouse. Let me walk you through the data flow."*
# MAGIC
# MAGIC ```
# MAGIC Network Elements (RAN, Transport, Core, Fiber)
# MAGIC     │
# MAGIC     ▼
# MAGIC Bronze Layer (Raw Data)
# MAGIC ├── PM Counters (5-min KPIs from every element)
# MAGIC ├── FM Alarms (SNMP traps, syslog events)
# MAGIC ├── Network Topology (element inventory + dependencies)
# MAGIC └── Trouble Tickets (historical incidents)
# MAGIC     │
# MAGIC     ▼
# MAGIC Silver Layer (Enriched)
# MAGIC ├── PM + Topology join (know which region, city, type)
# MAGIC ├── Rolling statistics (1h, 6h, 24h averages)
# MAGIC ├── Z-scores (how abnormal is this reading?)
# MAGIC └── Enriched alarms with severity scoring
# MAGIC     │
# MAGIC     ▼
# MAGIC Gold Layer (Analytics + ML)
# MAGIC ├── Network Health Scores (per region, per hour)
# MAGIC ├── Anomaly Detection (Isolation Forest)
# MAGIC ├── Alarm Correlation (temporal + topological)
# MAGIC └── Feature Tables (for model training)
# MAGIC     │
# MAGIC     ▼
# MAGIC AI/BI Dashboard → NOC Integration → Auto-Ticketing
# MAGIC ```
# MAGIC
# MAGIC **Key points to emphasize:**
# MAGIC - *"Your existing NMS/OSS feeds directly into Bronze — no rip-and-replace"*
# MAGIC - *"Unity Catalog governs everything — who can see what, lineage, audit trail"*
# MAGIC - *"The same platform does ETL, ML, and dashboards — no separate tools"*

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 3: Live Demo — The Data (5 min)
# MAGIC
# MAGIC ### → Open Notebook `01_data_generation`
# MAGIC
# MAGIC > *"For this demo, we've modeled SLT's network topology across your 5 key regions."*
# MAGIC
# MAGIC **Scroll to show the topology generation. Highlight:**
# MAGIC - 443 network elements: cell towers, routers, switches, fiber links, core nodes
# MAGIC - 5 regions: Western (Colombo, Negombo), Central (Kandy), Southern (Galle), Northern (Jaffna), Eastern (Trincomalee)
# MAGIC - Naming convention: `COL-ENB-001` (Colombo eNodeB #1), `KDY-RTR-003` (Kandy Router #3)
# MAGIC
# MAGIC > *"We've injected 18 realistic fault scenarios across 30 days — the kind of events your NOC deals with regularly."*
# MAGIC
# MAGIC **Read a few fault scenarios aloud:**
# MAGIC - *"Fiber cut on the Colombo-Negombo trunk due to construction"*
# MAGIC - *"Kandy Perahera festival causing cell overload from massive crowds"*
# MAGIC - *"Gradual transceiver degradation in Kandy — rising BER before failure"*
# MAGIC - *"DDoS attack causing core congestion in Colombo"*
# MAGIC
# MAGIC > *"The PM counters have realistic diurnal patterns — traffic peaks at 8-10pm Sri Lanka time, drops at 3-5am — just like your real network."*
# MAGIC
# MAGIC You can also quickly show the data counts:

# COMMAND ----------

CATALOG = "YOUR_CATALOG"
SCHEMA = "telco_network_fault_detection"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print("=== Demo Data Summary ===")
for table in ["bronze_network_topology", "bronze_pm_counters", "bronze_alarms", "bronze_trouble_tickets",
              "silver_pm_enriched", "silver_alarms_enriched", "gold_network_health", "gold_ml_features",
              "gold_anomaly_scores", "gold_correlated_incidents"]:
    try:
        count = spark.table(table).count()
        layer = table.split("_")[0].upper()
        print(f"  [{layer:6s}] {table:40s} {count:>12,} rows")
    except Exception as e:
        print(f"  [{table.split('_')[0].upper():6s}] {table:40s} (not yet populated)")

# COMMAND ----------

# Quick topology overview for the audience
display(spark.sql("""
SELECT region, element_type, COUNT(*) as count
FROM bronze_network_topology
GROUP BY region, element_type
ORDER BY region, element_type
"""))

# COMMAND ----------

# Show the 18 fault scenarios injected
display(spark.sql("""
SELECT ticket_id, priority, root_cause, affected_region,
  affected_customers, mttr_minutes, resolution_notes
FROM bronze_trouble_tickets
ORDER BY priority, created_at
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 4: Live Demo — The Dashboard (15 min)
# MAGIC
# MAGIC ### → Switch to the AI/BI Dashboard
# MAGIC
# MAGIC ## Page 1: Network Health Overview
# MAGIC
# MAGIC > *"At a glance: 443 network elements monitored across 5 regions. Average health score of 91 out of 100 — but watch how it dips during fault events."*
# MAGIC
# MAGIC **Point to the Health Trend line chart:**
# MAGIC > *"See these dips? Each one corresponds to a fault scenario. The Western region (Colombo) shows the most activity because it has the highest element density — just like your real network."*
# MAGIC
# MAGIC **Use the Region filter** to isolate Western region:
# MAGIC > *"If I'm a NOC manager for the Western Province, I can filter down to just my region."*
# MAGIC
# MAGIC ## Page 2: Alarm Analysis & Correlation ⭐ (THIS IS THE MONEY SLIDE)
# MAGIC
# MAGIC **Point to the 3 KPIs:**
# MAGIC > *"1,862 raw alarms over 30 days. Traditional NOC? Your operators see every single one. With Databricks correlation? 248 actionable incidents. That's a **7.5:1 reduction** in noise."*
# MAGIC
# MAGIC **Point to the Raw Alarms vs Correlated Incidents chart:**
# MAGIC > *"Look at the spike days — fiber cut events. Without correlation, your NOC gets flooded. With correlation, it's ONE incident with a clear root cause."*
# MAGIC
# MAGIC **Point to the Fault Type pie chart:**
# MAGIC > *"The system automatically classifies what type of fault each incident is. Your operators don't have to guess — they know immediately if it's a fiber issue, a capacity problem, or a hardware failure."*
# MAGIC
# MAGIC **Point to Top Impacted Elements:**
# MAGIC > *"We can instantly see which elements are your repeat offenders. These fiber links? They might need proactive maintenance or replacement."*
# MAGIC
# MAGIC **⏸ PAUSE for questions here — this is usually where customers engage.**
# MAGIC
# MAGIC ## Page 3: Incidents & MTTR
# MAGIC
# MAGIC **Point to the MTTR Comparison bar chart:**
# MAGIC > *"For every fault type, we compare reactive MTTR versus proactive MTTR."*
# MAGIC
# MAGIC > *"Take FIBER_CUT: Reactive MTTR is 266 minutes — 4+ hours of customer impact. With proactive detection, we pre-position splice teams when we see optical power degrading. Estimated MTTR drops to under 2 hours."*
# MAGIC
# MAGIC > *"TRANSCEIVER_DEGRADATION is even more dramatic. Today you only know when the link drops. With ML watching the BER trend, you schedule a replacement during a maintenance window — zero customer impact."*

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 5: Under the Hood — ML Models (10 min)
# MAGIC
# MAGIC ### → Open Notebook `03_anomaly_detection`
# MAGIC
# MAGIC > *"Let me show you how the ML works. We use two complementary approaches."*
# MAGIC
# MAGIC **Scroll to Isolation Forest section:**
# MAGIC > *"First: Isolation Forest. This is an unsupervised model — it doesn't need labeled data. It learns what 'normal' looks like for each element and flags deviations."*
# MAGIC
# MAGIC > *"Why this matters for SLT: You don't need years of perfectly labeled incident data to get started. Plug in your PM counters, and the model starts finding anomalies from day one."*
# MAGIC
# MAGIC **Show the classification report and proactive detection analysis.**
# MAGIC
# MAGIC **Open MLflow Experiment (briefly):**
# MAGIC > *"Every model is versioned, tracked, and reproducible in MLflow. When you retrain with new data, you can compare performance and promote the best model to production."*
# MAGIC
# MAGIC ### → Open Notebook `04_alarm_correlation`
# MAGIC
# MAGIC > *"The second piece: alarm correlation. When a fiber gets cut, 50+ downstream elements start alarming. Your NOC sees 50 alerts. Our algorithm sees 1 incident."*
# MAGIC
# MAGIC > *"We combine two signals: temporal proximity (alarms within 15 minutes) and topological dependency (elements that share upstream connections)."*
# MAGIC
# MAGIC You can show the live correlation stats:

# COMMAND ----------

# Alarm Correlation Effectiveness — live query
display(spark.sql("""
SELECT
  (SELECT COUNT(*) FROM bronze_alarms) as raw_alarms,
  (SELECT COUNT(*) FROM gold_correlated_incidents) as correlated_incidents,
  ROUND((SELECT COUNT(*) FROM bronze_alarms) * 1.0 /
    NULLIF((SELECT COUNT(*) FROM gold_correlated_incidents), 0), 1) as reduction_ratio,
  (SELECT SUM(raw_alarm_count) FROM gold_correlated_incidents) as alarms_in_incidents,
  (SELECT COUNT(*) FROM bronze_alarms) -
    (SELECT SUM(raw_alarm_count) FROM gold_correlated_incidents) as standalone_noise
"""))

# COMMAND ----------

# Top correlated incidents — show the power of root cause identification
display(spark.sql("""
SELECT incident_id, root_cause_element, root_cause_type,
  raw_alarm_count, affected_count, max_severity, region,
  estimated_customer_impact
FROM gold_correlated_incidents
ORDER BY raw_alarm_count DESC
LIMIT 10
"""))

# COMMAND ----------

# Anomaly detection results summary
display(spark.sql("""
SELECT severity, COUNT(*) as count,
  ROUND(AVG(anomaly_score), 4) as avg_score
FROM gold_anomaly_scores
WHERE is_anomaly = true
GROUP BY severity
ORDER BY severity
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 6: Business Value & Next Steps (10 min)
# MAGIC
# MAGIC ## Quantified Business Case
# MAGIC
# MAGIC | Metric | Current State | With Databricks |
# MAGIC |--------|--------------|-----------------|
# MAGIC | Alarm noise for NOC | 1,862 raw alarms/month | 248 correlated incidents (**7.5x reduction**) |
# MAGIC | MTTR | Avg ~150 min | Avg ~67 min (**55% reduction**) |
# MAGIC | Proactive detection | 0% (reactive only) | **60-80%** of faults caught early |
# MAGIC | Gradual degradation | Missed until failure | Detected **days in advance** |
# MAGIC | Customer impact | Learn from complaints | Fix before they notice |
# MAGIC
# MAGIC > *"For SLT with 7 million mobile subscribers and 500K+ fiber customers, even a 1% improvement in availability translates to significant revenue protection and NPS improvement."*
# MAGIC
# MAGIC ## SLT-Specific Value Hooks
# MAGIC
# MAGIC **Climate resilience:**
# MAGIC > *"After Cyclone Ditwah, you know how quickly environmental events cascade through the network. This system gives your NOC a 360-degree view — correlating fiber damage, power failures, and tower outages into a coherent incident picture, even during a storm."*
# MAGIC
# MAGIC **5G rollout:**
# MAGIC > *"As you expand 5G across Sri Lanka, the network gets more complex. More cells, more handovers, more potential failure points. Proactive monitoring becomes essential — you can't afford reactive operations at 5G scale."*
# MAGIC
# MAGIC **Competing with Dialog:**
# MAGIC > *"Dialog has 50%+ market share and AI-driven personalization. SLT's competitive edge is network quality — the fastest broadband in Sri Lanka. Proactive fault detection protects that advantage."*
# MAGIC
# MAGIC ## Proposed Next Steps
# MAGIC
# MAGIC 1. **Phase 1 (4-6 weeks): Proof of Concept**
# MAGIC    - Connect SLT's existing NMS/OSS data feeds (PM counters, FM alarms) to Databricks
# MAGIC    - Start with one region (suggest Western Province — highest density, most impact)
# MAGIC    - Train Isolation Forest on real data, measure anomaly detection accuracy
# MAGIC    - Validate alarm correlation against historical incidents
# MAGIC
# MAGIC 2. **Phase 2 (2-3 months): Production Pilot**
# MAGIC    - Expand to all 5 provinces
# MAGIC    - Integrate with SLT's ticketing system (auto-create tickets for correlated incidents)
# MAGIC    - Add real-time streaming (Structured Streaming from Kafka/SNMP)
# MAGIC    - Train supervised models using SLT's historical trouble ticket data
# MAGIC
# MAGIC 3. **Phase 3 (Ongoing): Full Production**
# MAGIC    - NOC dashboard integration (embed in existing SLT NOC tools)
# MAGIC    - Automated remediation playbooks (e.g., auto-reroute traffic on predicted fiber degradation)
# MAGIC    - Extend to 5G RAN analytics as rollout progresses
# MAGIC    - Add customer experience correlation (QoE metrics + network faults)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Handling Common Questions
# MAGIC
# MAGIC **Q: "How does this integrate with our existing Huawei/Ericsson NMS?"**
# MAGIC > *"Databricks sits alongside your NMS, not replacing it. Your NMS continues to collect data — we ingest the PM counters and alarms via standard interfaces (SNMP, file export, Kafka, or REST APIs). Think of Databricks as the analytics brain on top of your operational tools."*
# MAGIC
# MAGIC **Q: "What about real-time? This demo uses batch data."**
# MAGIC > *"In production, we'd use Spark Structured Streaming for near-real-time ingestion — processing alarms within seconds of arrival. The demo uses batch for simplicity, but the exact same code runs in streaming mode with minimal changes."*
# MAGIC
# MAGIC **Q: "How much data do we need to start?"**
# MAGIC > *"The Isolation Forest model is unsupervised — it needs about 2-4 weeks of PM counter data to learn normal patterns. No labeled data required. For the supervised fault classifier, historical trouble tickets with root cause labels improve accuracy, but they're not required to get value from day one."*
# MAGIC
# MAGIC **Q: "What about false positives? Our NOC is already overwhelmed."**
# MAGIC > *"That's exactly what alarm correlation solves. The raw anomaly detector may have some false positives, but the correlation engine filters noise — only surfacing incidents where multiple signals converge. In our demo, we went from 1,862 raw signals to 248 actionable incidents."*
# MAGIC
# MAGIC **Q: "Can we do this on-premises?"**
# MAGIC > *"Databricks runs on AWS, Azure, or GCP. Given Sri Lanka's data residency preferences, we can deploy in the nearest region (Mumbai or Singapore). If SLT has a private cloud, we can discuss hybrid architectures."*
# MAGIC
# MAGIC **Q: "What's the cost?"**
# MAGIC > *"For a POC, we'd use a small serverless SQL warehouse and a single-node cluster for ML training. The ROI comes from reduced MTTR — even preventing one P1 outage per quarter pays for the platform many times over."*

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo Environment Quick Reference
# MAGIC
# MAGIC | Resource | Location |
# MAGIC |----------|----------|
# MAGIC | Workspace | This workspace |
# MAGIC | Catalog | `YOUR_CATALOG` |
# MAGIC | Schema | `telco_network_fault_detection` |
# MAGIC | Notebooks | `telco_network_fault_detection/` (00 through 04) |
# MAGIC | Dashboard | Dashboards → "Sri Lanka Telecom - Network Fault Detection" |
# MAGIC | MLflow | Experiments → `telco_fault_detection_experiment` |
# MAGIC | Warehouse | Serverless Starter Warehouse (auto-starts) |

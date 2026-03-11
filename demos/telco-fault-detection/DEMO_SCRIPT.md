# Proactive Network Fault Detection — Demo Script
## Sri Lanka Telecom (SLT-Mobitel)

**Duration:** 45-60 minutes
**Audience:** SLT Network Operations, CTO Office, Digital Transformation Team
**Presenter:** [Your Name], Databricks

---

## Pre-Demo Setup (10 min before)

1. Open these tabs in your browser:
   - **Dashboard**: Navigate to `Dashboards` → "Sri Lanka Telecom - Network Fault Detection"
   - **Notebook 01** (Data Generation): `/telco_network_fault_detection/01_data_generation`
   - **Notebook 03** (Anomaly Detection): `/telco_network_fault_detection/03_anomaly_detection`
   - **Notebook 04** (Alarm Correlation): `/telco_network_fault_detection/04_alarm_correlation`
   - **MLflow Experiment**: `/telco_fault_detection_experiment`
2. Ensure the dashboard is loaded and showing data
3. Have the architecture slide ready (or sketch on whiteboard)

---

## Part 1: Setting the Context (5 min)

### Opening — Make It About Them

> "SLT-Mobitel operates one of the most critical infrastructure networks in Sri Lanka — 45,000 km of fiber, 7 million mobile subscribers, and you've just launched 5G in Colombo. Your network is the backbone of Sri Lanka's digital transformation."
>
> "But with this growth comes complexity. After Cyclone Ditwah last year, we saw how climate events can cascade through the network — fiber cuts, tower outages, power failures hitting multiple regions simultaneously. Your NOC teams dealt with thousands of alarms in hours."
>
> "Today I want to show you how Databricks can help SLT move from **reactive** firefighting to **proactive** fault detection — catching problems before your customers even notice."

### The Problem Statement (draw on whiteboard or show slide)

| Today (Reactive) | Tomorrow (Proactive) |
|---|---|
| Alarm fires → NOC investigates | ML detects anomaly → Pre-staged response |
| 1000s of alarms in a storm → Alert fatigue | Smart correlation → 1 actionable incident |
| Static thresholds miss gradual degradation | ML catches transceiver aging weeks early |
| MTTR: hours | MTTR: minutes |
| Customers call you about outages | You fix it before customers notice |

---

## Part 2: Architecture Walkthrough (5 min)

### Show the Medallion Architecture

> "The solution runs entirely on the Databricks Lakehouse. Let me walk you through the data flow."

**Draw or show:**
```
Network Elements (RAN, Transport, Core, Fiber)
    │
    ▼
Bronze Layer (Raw Data)
├── PM Counters (5-min KPIs from every element)
├── FM Alarms (SNMP traps, syslog events)
├── Network Topology (element inventory + dependencies)
└── Trouble Tickets (historical incidents)
    │
    ▼
Silver Layer (Enriched)
├── PM + Topology join (know which region, city, type)
├── Rolling statistics (1h, 6h, 24h averages)
├── Z-scores (how abnormal is this reading?)
└── Enriched alarms with severity scoring
    │
    ▼
Gold Layer (Analytics + ML)
├── Network Health Scores (per region, per hour)
├── Anomaly Detection (Isolation Forest)
├── Alarm Correlation (temporal + topological)
└── Feature Tables (for model training)
    │
    ▼
AI/BI Dashboard → NOC Integration → Auto-Ticketing
```

**Key points to emphasize:**
- "Your existing NMS/OSS feeds directly into Bronze — no rip-and-replace"
- "Unity Catalog governs everything — who can see what, lineage, audit trail"
- "The same platform does ETL, ML, and dashboards — no separate tools"

---

## Part 3: Live Demo — The Data (5 min)

### Open Notebook 01 (Data Generation)

> "For this demo, we've modeled SLT's network topology across your 5 key regions."

**Scroll to show the topology generation. Highlight:**
- 443 network elements: cell towers, routers, switches, fiber links, core nodes
- 5 regions: Western (Colombo, Negombo), Central (Kandy), Southern (Galle), Northern (Jaffna), Eastern (Trincomalee)
- Naming convention: `COL-ENB-001` (Colombo eNodeB #1), `KDY-RTR-003` (Kandy Router #3)

> "We've injected 18 realistic fault scenarios across 30 days — the kind of events your NOC deals with regularly."

**Scroll to the fault scenarios list and read a few:**
- "Fiber cut on the Colombo-Negombo trunk due to construction"
- "Kandy Perahera festival causing cell overload from massive crowds"
- "Gradual transceiver degradation in Kandy — rising BER before failure"
- "DDoS attack causing core congestion in Colombo"

> "The PM counters have realistic diurnal patterns — traffic peaks at 8-10pm Sri Lanka time, drops at 3-5am — just like your real network."

---

## Part 4: Live Demo — The Dashboard (15 min)

### Page 1: Network Health Overview

**Open the dashboard. Start with KPIs:**

> "At a glance: 443 network elements monitored across 5 regions. Average health score of 91 out of 100 — but watch how it dips during fault events."

**Point to the Health Trend line chart:**
> "See these dips? Each one corresponds to a fault scenario. The Western region (Colombo) shows the most activity because it has the highest element density — just like your real network."

> "Now look at the regional comparison. Central and Southern show more alarms — that's our simulated fiber link flapping on the Kandy-Matale link and the Galle flooding event."

**Use the Region filter** to isolate Western region:
> "If I'm a NOC manager for the Western Province, I can filter down to just my region."

### Page 2: Alarm Analysis & Correlation (THIS IS THE MONEY SLIDE)

**Switch to Page 2.**

> "Here's where Databricks transforms your NOC operations."

**Point to the 3 KPIs:**
> "1,862 raw alarms over 30 days. Traditional NOC? Your operators see every single one. With Databricks correlation? 248 actionable incidents. That's a **7.5:1 reduction** in noise."

**Point to the Raw Alarms vs Correlated Incidents chart:**
> "Look at February 11 — a spike of alarms from the fiber cut event. Without correlation, your NOC gets flooded. With correlation, it's ONE incident with a clear root cause."

**Point to the Fault Type pie chart:**
> "The system automatically classifies what type of fault each incident is. Your operators don't have to guess — they know immediately if it's a fiber issue, a capacity problem, or a hardware failure."

**Point to Top Impacted Elements:**
> "And we can instantly see which elements are your repeat offenders. These Matale and Galle fiber links? They might need proactive maintenance or replacement."

**PAUSE for questions here — this is usually where customers engage.**

### Page 3: Incidents & MTTR

**Switch to Page 3.**

> "The ultimate business case: Mean Time To Repair."

**Point to the MTTR Comparison bar chart:**
> "For every fault type, we compare reactive MTTR — how long it takes today — versus proactive MTTR — how long it takes when ML gives you advance warning."

> "Take FIBER_CUT: Reactive MTTR is 266 minutes. That's 4+ hours of customer impact. With proactive detection, we can pre-position splice teams when we see optical power degrading. Estimated MTTR drops to under 2 hours."

> "TRANSCEIVER_DEGRADATION is even more dramatic. Today you only know when the link drops. With ML watching the BER trend, you can schedule a replacement during a maintenance window — zero customer impact."

**Point to the Incidents Table:**
> "Every incident has a root cause element identified, the number of raw alarms it absorbed, severity, and estimated customer impact. This is what your NOC dashboard would look like — actionable, prioritized, and noise-free."

---

## Part 5: Under the Hood — ML Models (10 min)

### Open Notebook 03 (Anomaly Detection)

> "Let me show you how the ML works. We use two complementary approaches."

**Scroll to Isolation Forest section:**
> "First: Isolation Forest. This is an unsupervised model — it doesn't need labeled data. It learns what 'normal' looks like for each element and flags deviations."

> "Why this matters for SLT: You don't need years of perfectly labeled incident data to get started. Plug in your PM counters, and the model starts finding anomalies from day one."

**Show the classification report output:**
> "Against our known fault scenarios, the model achieves [X]% recall — meaning it catches [X]% of faults. And more importantly, it catches gradual degradation events that static thresholds completely miss."

**Scroll to the Proactive Detection Analysis:**
> "This is the key result: for gradual faults like transceiver degradation and memory leaks, the model detects anomalies **hours or even days** before the actual outage. That's your window to act proactively."

**Open MLflow Experiment (briefly):**
> "Every model is versioned, tracked, and reproducible in MLflow. When you retrain with new data, you can compare performance and promote the best model to production."

### Open Notebook 04 (Alarm Correlation)

> "The second piece: alarm correlation. When a fiber gets cut, 50+ downstream elements start alarming. Your NOC sees 50 alerts. Our algorithm sees 1 incident."

**Scroll to the correlation algorithm section:**
> "We combine two signals: temporal proximity (alarms within 15 minutes) and topological dependency (elements that share upstream connections). The result is that single root cause identification you saw in the dashboard."

---

## Part 6: Business Value & Next Steps (10 min)

### Quantify the Value

> "Let me summarize the business case for SLT:"

| Metric | Current State | With Databricks |
|--------|--------------|-----------------|
| Alarm noise for NOC | 1,862 raw alarms/month | 248 correlated incidents (7.5x reduction) |
| MTTR | Avg ~150 min | Avg ~67 min (55% reduction) |
| Proactive detection | 0% (reactive only) | 60-80% of faults caught early |
| Gradual degradation | Missed until failure | Detected days in advance |
| Customer impact | Learn from complaints | Fix before they notice |

> "For SLT with 7 million mobile subscribers and 500K+ fiber customers, even a 1% improvement in availability translates to significant revenue protection and NPS improvement."

### SLT-Specific Value Hooks

**Climate resilience:**
> "After Cyclone Ditwah, you know how quickly environmental events cascade through the network. This system gives your NOC a 360-degree view — correlating fiber damage, power failures, and tower outages into a coherent incident picture, even during a storm."

**5G rollout:**
> "As you expand 5G across Sri Lanka, the network gets more complex. More cells, more handovers, more potential failure points. Proactive monitoring becomes essential — you can't afford reactive operations at 5G scale."

**Competing with Dialog:**
> "Dialog has 50%+ market share and AI-driven personalization. SLT's competitive edge is network quality — the fastest broadband in Sri Lanka. Proactive fault detection protects that advantage."

### Proposed Next Steps

> "Here's what a real implementation would look like:"

1. **Phase 1 (4-6 weeks): Proof of Concept**
   - Connect SLT's existing NMS/OSS data feeds (PM counters, FM alarms) to Databricks
   - Start with one region (suggest Western Province — highest density, most impact)
   - Train Isolation Forest on real data, measure anomaly detection accuracy
   - Validate alarm correlation against historical incidents

2. **Phase 2 (2-3 months): Production Pilot**
   - Expand to all 5 provinces
   - Integrate with SLT's ticketing system (auto-create tickets for correlated incidents)
   - Add real-time streaming (Structured Streaming from Kafka/SNMP)
   - Train supervised models using SLT's historical trouble ticket data

3. **Phase 3 (Ongoing): Full Production**
   - NOC dashboard integration (embed in existing SLT NOC tools)
   - Automated remediation playbooks (e.g., auto-reroute traffic on predicted fiber degradation)
   - Extend to 5G RAN analytics as rollout progresses
   - Add customer experience correlation (QoE metrics + network faults)

---

## Handling Common Questions

**Q: "How does this integrate with our existing Huawei/Ericsson NMS?"**
> "Databricks sits alongside your NMS, not replacing it. Your NMS continues to collect data — we ingest the PM counters and alarms via standard interfaces (SNMP, file export, Kafka, or REST APIs). Think of Databricks as the analytics brain on top of your operational tools."

**Q: "What about real-time? This demo uses batch data."**
> "Great question. In production, we'd use Spark Structured Streaming for near-real-time ingestion — processing alarms within seconds of arrival. The demo uses batch for simplicity, but the exact same code runs in streaming mode with minimal changes."

**Q: "How much data do we need to start?"**
> "The Isolation Forest model is unsupervised — it needs about 2-4 weeks of PM counter data to learn normal patterns. No labeled data required. For the supervised fault classifier, historical trouble tickets with root cause labels improve accuracy, but they're not required to get value from day one."

**Q: "What about false positives? Our NOC is already overwhelmed."**
> "That's exactly what alarm correlation solves. The raw anomaly detector may have some false positives, but the correlation engine filters noise — only surfacing incidents where multiple signals converge. In our demo, we went from 1,862 raw signals to 248 actionable incidents. Your operators see fewer, better alerts."

**Q: "Can we do this on-premises?"**
> "Databricks runs on AWS, Azure, or GCP. Given Sri Lanka's data residency preferences, we can deploy in the nearest region (Mumbai or Singapore). If SLT has a private cloud, we can discuss hybrid architectures."

**Q: "What's the cost?"**
> "For a POC, we'd typically use a small serverless SQL warehouse and a single-node cluster for ML training — that's roughly $X/month (work with your Databricks AE for specific pricing). The ROI comes from reduced MTTR — even preventing one P1 outage per quarter pays for the platform many times over."

---

## Demo Environment Quick Reference

| Resource | Location |
|----------|----------|
| Workspace | Your Databricks workspace URL |
| Catalog | Update `YOUR_CATALOG` in each notebook to your catalog name |
| Schema | `telco_network_fault_detection` |
| Notebooks | `/Users/<your-email>/telco_network_fault_detection/` |
| Dashboard | Dashboards → "Sri Lanka Telecom - Network Fault Detection" |
| MLflow | Experiments → `telco_fault_detection_experiment` |
| Warehouse | Any serverless SQL warehouse |

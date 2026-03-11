# Databricks notebook source
# MAGIC %md
# MAGIC # Proactive Network Fault Detection — Synthetic Data Generation
# MAGIC ### Sri Lanka Telecom Demo
# MAGIC
# MAGIC Generates realistic telecom network data including:
# MAGIC - Network topology (~500 elements across 5 regions)
# MAGIC - Performance metrics (PM counters) at 5-min intervals for 30 days
# MAGIC - Fault management alarms with injected fault patterns
# MAGIC - Historical trouble tickets with root cause labels
# MAGIC
# MAGIC **Key realism features:**
# MAGIC - Diurnal traffic patterns (peak 8-10pm, low 3-5am Sri Lanka time)
# MAGIC - 18 injected fault scenarios (fiber cuts, transceiver degradation, cell overload, etc.)
# MAGIC - Cascading alarm patterns from topology dependencies
# MAGIC - Proper Sri Lankan geography and naming conventions

# COMMAND ----------

CATALOG = "YOUR_CATALOG"
SCHEMA = "telco_network_fault_detection"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

import random
import uuid
import json
from datetime import datetime, timedelta
import math
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import functions as F

random.seed(42)

# Time range: 30 days
START_DATE = datetime(2026, 2, 8, 0, 0, 0)
END_DATE = datetime(2026, 3, 10, 0, 0, 0)
INTERVAL_MINUTES = 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Network Topology Generation
# MAGIC
# MAGIC Sri Lanka Telecom network hierarchy:
# MAGIC ```
# MAGIC CoreNode (2 per region) → Router → Switch → eNodeB (cell towers)
# MAGIC                                   → FiberLink (transport)
# MAGIC ```

# COMMAND ----------

# Sri Lankan regions with cities and coordinates
REGIONS = {
    "Western": {
        "cities": [
            ("Colombo", 6.9271, 79.8612),
            ("Negombo", 7.2083, 79.8358),
            ("Moratuwa", 6.7734, 79.8824),
            ("Dehiwala", 6.8510, 79.8652),
        ],
        "density": 1.5  # Higher density = more elements
    },
    "Central": {
        "cities": [
            ("Kandy", 7.2906, 80.6337),
            ("Matale", 7.4675, 80.6234),
            ("Nuwara Eliya", 6.9497, 80.7891),
        ],
        "density": 1.0
    },
    "Southern": {
        "cities": [
            ("Galle", 6.0535, 80.2210),
            ("Matara", 5.9549, 80.5550),
            ("Hambantota", 6.1249, 81.1185),
        ],
        "density": 0.8
    },
    "Northern": {
        "cities": [
            ("Jaffna", 9.6615, 80.0255),
            ("Kilinochchi", 9.3803, 80.3770),
            ("Vavuniya", 8.7514, 80.4971),
        ],
        "density": 0.7
    },
    "Eastern": {
        "cities": [
            ("Trincomalee", 8.5874, 81.2152),
            ("Batticaloa", 7.7310, 81.6747),
            ("Ampara", 7.2975, 81.6820),
        ],
        "density": 0.7
    }
}

VENDORS = {
    "eNodeB": [("Huawei", "BTS3900"), ("Ericsson", "RBS6601"), ("Nokia", "Flexi BTS")],
    "Router": [("Cisco", "ASR9000"), ("Juniper", "MX480"), ("Huawei", "NE40E")],
    "Switch": [("Cisco", "Nexus9000"), ("Huawei", "CE6800"), ("Juniper", "QFX5100")],
    "FiberLink": [("Corning", "SMF-28e"), ("Fujikura", "FutureGuide-SR15"), ("Prysmian", "FlexTube")],
    "CoreNode": [("Ericsson", "SGSN-MME"), ("Nokia", "ATCA-7210"), ("Huawei", "USN9810")]
}

# Region code mapping
REGION_CODES = {"Western": "COL", "Central": "KDY", "Southern": "GAL", "Northern": "JAF", "Eastern": "TRC"}
ELEMENT_CODES = {"eNodeB": "ENB", "Router": "RTR", "Switch": "SWT", "FiberLink": "FBR", "CoreNode": "COR"}

# COMMAND ----------

elements = []
element_id_counter = {}

def next_id(region, etype):
    key = f"{REGION_CODES[region]}-{ELEMENT_CODES[etype]}"
    element_id_counter[key] = element_id_counter.get(key, 0) + 1
    return f"{key}-{element_id_counter[key]:03d}"

# Generate topology per region
for region, config in REGIONS.items():
    density = config["density"]

    for city, lat, lon in config["cities"]:
        # 2 core nodes per major city in Western, 1 elsewhere
        n_core = 2 if region == "Western" and city == "Colombo" else 1
        core_ids = []
        for _ in range(n_core):
            eid = next_id(region, "CoreNode")
            vendor, model = random.choice(VENDORS["CoreNode"])
            elements.append({
                "element_id": eid, "element_type": "CoreNode",
                "element_name": f"{city} Core {eid[-3:]}",
                "region": region, "city": city,
                "latitude": lat + random.uniform(-0.01, 0.01),
                "longitude": lon + random.uniform(-0.01, 0.01),
                "parent_element_id": None,
                "vendor": vendor, "model": model,
                "install_date": f"20{random.randint(15,22)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                "status": "Active",
                "capacity_gbps": random.choice([100.0, 200.0, 400.0]),
                "created_at": START_DATE.isoformat()
            })
            core_ids.append(eid)

        # 2-4 routers per city
        n_routers = int(random.randint(2, 4) * density)
        router_ids = []
        for _ in range(n_routers):
            eid = next_id(region, "Router")
            vendor, model = random.choice(VENDORS["Router"])
            parent = random.choice(core_ids)
            elements.append({
                "element_id": eid, "element_type": "Router",
                "element_name": f"{city} Router {eid[-3:]}",
                "region": region, "city": city,
                "latitude": lat + random.uniform(-0.05, 0.05),
                "longitude": lon + random.uniform(-0.05, 0.05),
                "parent_element_id": parent,
                "vendor": vendor, "model": model,
                "install_date": f"20{random.randint(16,23)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                "status": "Active",
                "capacity_gbps": random.choice([10.0, 40.0, 100.0]),
                "created_at": START_DATE.isoformat()
            })
            router_ids.append(eid)

        # 2-3 switches per router
        switch_ids = []
        for rid in router_ids:
            n_switches = int(random.randint(1, 3) * density)
            for _ in range(n_switches):
                eid = next_id(region, "Switch")
                vendor, model = random.choice(VENDORS["Switch"])
                elements.append({
                    "element_id": eid, "element_type": "Switch",
                    "element_name": f"{city} Switch {eid[-3:]}",
                    "region": region, "city": city,
                    "latitude": lat + random.uniform(-0.08, 0.08),
                    "longitude": lon + random.uniform(-0.08, 0.08),
                    "parent_element_id": rid,
                    "vendor": vendor, "model": model,
                    "install_date": f"20{random.randint(17,24)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                    "status": "Active",
                    "capacity_gbps": random.choice([1.0, 10.0, 25.0]),
                    "created_at": START_DATE.isoformat()
                })
                switch_ids.append(eid)

        # 3-6 eNodeBs per switch
        for sid in switch_ids:
            n_enb = int(random.randint(2, 5) * density)
            for _ in range(n_enb):
                eid = next_id(region, "eNodeB")
                vendor, model = random.choice(VENDORS["eNodeB"])
                elements.append({
                    "element_id": eid, "element_type": "eNodeB",
                    "element_name": f"{city} Cell {eid[-3:]}",
                    "region": region, "city": city,
                    "latitude": lat + random.uniform(-0.15, 0.15),
                    "longitude": lon + random.uniform(-0.15, 0.15),
                    "parent_element_id": sid,
                    "vendor": vendor, "model": model,
                    "install_date": f"20{random.randint(18,25)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                    "status": "Active",
                    "capacity_gbps": random.choice([0.1, 0.3, 1.0]),
                    "created_at": START_DATE.isoformat()
                })

        # Fiber links between routers
        for i, rid in enumerate(router_ids):
            eid = next_id(region, "FiberLink")
            vendor, model = random.choice(VENDORS["FiberLink"])
            target = core_ids[i % len(core_ids)]
            elements.append({
                "element_id": eid, "element_type": "FiberLink",
                "element_name": f"{city} Fiber {eid[-3:]}",
                "region": region, "city": city,
                "latitude": lat + random.uniform(-0.03, 0.03),
                "longitude": lon + random.uniform(-0.03, 0.03),
                "parent_element_id": target,
                "vendor": vendor, "model": model,
                "install_date": f"20{random.randint(14,21)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                "status": "Active",
                "capacity_gbps": random.choice([10.0, 40.0, 100.0]),
                "created_at": START_DATE.isoformat()
            })

print(f"Generated {len(elements)} network elements")
type_counts = {}
for e in elements:
    type_counts[e['element_type']] = type_counts.get(e['element_type'], 0) + 1
for t, c in sorted(type_counts.items()):
    print(f"  {t}: {c}")

# COMMAND ----------

# Write topology to bronze table
topology_schema = StructType([
    StructField("element_id", StringType()), StructField("element_type", StringType()),
    StructField("element_name", StringType()), StructField("region", StringType()),
    StructField("city", StringType()), StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()), StructField("parent_element_id", StringType()),
    StructField("vendor", StringType()), StructField("model", StringType()),
    StructField("install_date", StringType()), StructField("status", StringType()),
    StructField("capacity_gbps", DoubleType()), StructField("created_at", StringType())
])

topology_df = spark.createDataFrame([Row(**e) for e in elements], schema=topology_schema)
topology_df = topology_df.withColumn("install_date", F.to_date("install_date")) \
                         .withColumn("created_at", F.to_timestamp("created_at"))

topology_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_network_topology")
print(f"✓ Wrote {topology_df.count()} elements to bronze_network_topology")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define Fault Scenarios
# MAGIC
# MAGIC We inject 18 realistic fault patterns across the 30-day window. Each fault affects specific elements
# MAGIC and manifests as both KPI degradation and alarm storms.

# COMMAND ----------

# Build lookup structures
elements_by_type = {}
elements_by_region = {}
parent_map = {}
children_map = {}

for e in elements:
    etype = e["element_type"]
    elements_by_type.setdefault(etype, []).append(e)
    elements_by_region.setdefault(e["region"], {}).setdefault(etype, []).append(e)
    parent_map[e["element_id"]] = e.get("parent_element_id")
    children_map.setdefault(e.get("parent_element_id"), []).append(e["element_id"])

def get_downstream(element_id, depth=3):
    """Get all downstream elements up to given depth."""
    result = []
    queue = [element_id]
    for _ in range(depth):
        next_queue = []
        for eid in queue:
            for child in children_map.get(eid, []):
                result.append(child)
                next_queue.append(child)
        queue = next_queue
    return result

# Define 18 fault scenarios
FAULT_SCENARIOS = [
    # Fiber cuts — sudden, cascading impact
    {"id": "F01", "type": "FIBER_CUT", "region": "Western", "start": START_DATE + timedelta(days=3, hours=14),
     "duration_h": 6, "element_type": "FiberLink", "severity": "CRITICAL",
     "description": "Fiber cut on Colombo-Negombo trunk due to construction"},
    {"id": "F02", "type": "FIBER_CUT", "region": "Southern", "start": START_DATE + timedelta(days=10, hours=9),
     "duration_h": 8, "element_type": "FiberLink", "severity": "CRITICAL",
     "description": "Fiber cut in Galle coastal area due to flooding"},
    {"id": "F03", "type": "FIBER_CUT", "region": "Northern", "start": START_DATE + timedelta(days=22, hours=16),
     "duration_h": 12, "element_type": "FiberLink", "severity": "CRITICAL",
     "description": "Fiber damage on Jaffna ring — road works"},

    # Transceiver degradation — gradual, detectable early by ML
    {"id": "F04", "type": "TRANSCEIVER_DEGRADATION", "region": "Western", "start": START_DATE + timedelta(days=5),
     "duration_h": 120, "element_type": "FiberLink", "severity": "MAJOR",
     "description": "Gradual optical power degradation on Colombo metro fiber"},
    {"id": "F05", "type": "TRANSCEIVER_DEGRADATION", "region": "Central", "start": START_DATE + timedelta(days=15),
     "duration_h": 168, "element_type": "FiberLink", "severity": "MAJOR",
     "description": "Aging transceiver in Kandy — rising BER before failure"},

    # Cell tower overload — capacity events (e.g., festivals, cricket matches)
    {"id": "F06", "type": "CELL_OVERLOAD", "region": "Central", "start": START_DATE + timedelta(days=7, hours=18),
     "duration_h": 5, "element_type": "eNodeB", "severity": "MAJOR",
     "description": "Kandy Perahera festival — massive crowd causing cell congestion"},
    {"id": "F07", "type": "CELL_OVERLOAD", "region": "Western", "start": START_DATE + timedelta(days=14, hours=14),
     "duration_h": 4, "element_type": "eNodeB", "severity": "MAJOR",
     "description": "Cricket match at R. Premadasa Stadium — Colombo cell overload"},
    {"id": "F08", "type": "CELL_OVERLOAD", "region": "Southern", "start": START_DATE + timedelta(days=20, hours=19),
     "duration_h": 3, "element_type": "eNodeB", "severity": "MINOR",
     "description": "Galle beach festival event — localized congestion"},

    # Router/switch failures — hardware issues
    {"id": "F09", "type": "CPU_OVERLOAD", "region": "Western", "start": START_DATE + timedelta(days=4, hours=10),
     "duration_h": 3, "element_type": "Router", "severity": "CRITICAL",
     "description": "BGP route leak causing CPU spike on Colombo core router"},
    {"id": "F10", "type": "MEMORY_LEAK", "region": "Eastern", "start": START_DATE + timedelta(days=12),
     "duration_h": 48, "element_type": "Router", "severity": "MAJOR",
     "description": "Software bug causing gradual memory leak on Trincomalee router"},
    {"id": "F11", "type": "POWER_SUPPLY_FAILURE", "region": "Northern", "start": START_DATE + timedelta(days=17, hours=2),
     "duration_h": 4, "element_type": "Switch", "severity": "CRITICAL",
     "description": "Power supply unit failure at Jaffna aggregation switch"},

    # Temperature/environmental
    {"id": "F12", "type": "TEMPERATURE_HIGH", "region": "Eastern", "start": START_DATE + timedelta(days=8, hours=12),
     "duration_h": 8, "element_type": "eNodeB", "severity": "MAJOR",
     "description": "AC failure at Batticaloa cell site — equipment overheating"},
    {"id": "F13", "type": "TEMPERATURE_HIGH", "region": "Western", "start": START_DATE + timedelta(days=25, hours=11),
     "duration_h": 6, "element_type": "Router", "severity": "MINOR",
     "description": "Cooling issue in Colombo data center rack"},

    # Intermittent link flapping
    {"id": "F14", "type": "LINK_FLAP", "region": "Central", "start": START_DATE + timedelta(days=9, hours=6),
     "duration_h": 24, "element_type": "FiberLink", "severity": "MAJOR",
     "description": "Intermittent fiber flapping on Kandy-Matale link — loose connector"},
    {"id": "F15", "type": "LINK_FLAP", "region": "Southern", "start": START_DATE + timedelta(days=18, hours=22),
     "duration_h": 10, "element_type": "FiberLink", "severity": "MINOR",
     "description": "Microfiber bend causing intermittent loss on Matara link"},

    # Core node issues
    {"id": "F16", "type": "CORE_CONGESTION", "region": "Western", "start": START_DATE + timedelta(days=21, hours=20),
     "duration_h": 3, "element_type": "CoreNode", "severity": "CRITICAL",
     "description": "DDoS attack causing core congestion in Colombo"},

    # Radio degradation
    {"id": "F17", "type": "RADIO_DEGRADATION", "region": "Eastern", "start": START_DATE + timedelta(days=16),
     "duration_h": 72, "element_type": "eNodeB", "severity": "MINOR",
     "description": "Antenna tilt drift on Ampara towers after maintenance error"},
    {"id": "F18", "type": "HANDOVER_FAILURE", "region": "Western", "start": START_DATE + timedelta(days=26, hours=8),
     "duration_h": 16, "element_type": "eNodeB", "severity": "MAJOR",
     "description": "Software update causing handover failures across Moratuwa cells"},
]

print(f"Defined {len(FAULT_SCENARIOS)} fault scenarios")
for f in FAULT_SCENARIOS:
    print(f"  [{f['id']}] {f['type']:30s} {f['region']:10s} {f['start'].strftime('%b %d %H:%M')} ({f['duration_h']}h) — {f['description'][:60]}")

# COMMAND ----------

# Pick specific affected elements for each fault
fault_elements = {}
for fault in FAULT_SCENARIOS:
    region_elements = elements_by_region.get(fault["region"], {}).get(fault["element_type"], [])
    if not region_elements:
        continue
    # Pick 1-3 primary elements depending on fault type
    n_primary = 1 if fault["type"] in ("FIBER_CUT", "POWER_SUPPLY_FAILURE") else random.randint(2, min(5, len(region_elements)))
    primary = random.sample(region_elements, min(n_primary, len(region_elements)))
    primary_ids = [e["element_id"] for e in primary]

    # Get downstream elements (cascading impact)
    downstream = []
    for pid in primary_ids:
        downstream.extend(get_downstream(pid))

    fault_elements[fault["id"]] = {
        "primary": primary_ids,
        "downstream": list(set(downstream)),
        "all_affected": list(set(primary_ids + downstream))
    }

print("Fault element mapping:")
for fid, mapping in fault_elements.items():
    print(f"  {fid}: {len(mapping['primary'])} primary, {len(mapping['downstream'])} downstream = {len(mapping['all_affected'])} total affected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Performance Metrics (PM Counters) Generation
# MAGIC
# MAGIC Generate 5-minute interval KPIs with:
# MAGIC - Realistic diurnal patterns (Sri Lanka is UTC+5:30)
# MAGIC - Normal noise and variance
# MAGIC - Fault injection causing KPI degradation

# COMMAND ----------

def diurnal_factor(hour):
    """Traffic pattern: peak at 20:00-22:00 LKT, low at 03:00-05:00."""
    # Shift to Sri Lanka time (UTC+5:30) — our timestamps are UTC
    lk_hour = (hour + 5) % 24  # Approximate
    # Smooth diurnal curve
    return 0.3 + 0.7 * (0.5 + 0.5 * math.sin(math.pi * (lk_hour - 5) / 12)) if 5 <= lk_hour <= 23 else 0.3 + 0.15 * max(0, math.sin(math.pi * lk_hour / 5))

def is_in_fault_window(element_id, timestamp, fault_id):
    """Check if element is affected by a fault at given time."""
    fault = next((f for f in FAULT_SCENARIOS if f["id"] == fault_id), None)
    if not fault or fault_id not in fault_elements:
        return False, False
    end_time = fault["start"] + timedelta(hours=fault["duration_h"])
    if not (fault["start"] <= timestamp <= end_time):
        return False, False
    mapping = fault_elements[fault_id]
    is_primary = element_id in mapping["primary"]
    is_downstream = element_id in mapping["downstream"]
    return is_primary, is_downstream

def get_active_faults(element_id, timestamp):
    """Get all active faults affecting this element."""
    active = []
    for fault in FAULT_SCENARIOS:
        is_primary, is_downstream = is_in_fault_window(element_id, timestamp, fault["id"])
        if is_primary or is_downstream:
            active.append((fault, is_primary))
    return active

# COMMAND ----------

# Metric definitions per element type
METRIC_DEFS = {
    "eNodeB": [
        {"name": "rsrp_dbm", "unit": "dBm", "base": -85, "noise": 5, "type": "GAUGE"},
        {"name": "rsrq_db", "unit": "dB", "base": -10, "noise": 2, "type": "GAUGE"},
        {"name": "throughput_dl_mbps", "unit": "Mbps", "base": 50, "noise": 15, "type": "GAUGE", "diurnal": True},
        {"name": "throughput_ul_mbps", "unit": "Mbps", "base": 15, "noise": 5, "type": "GAUGE", "diurnal": True},
        {"name": "handover_success_rate", "unit": "%", "base": 98.5, "noise": 1.0, "type": "GAUGE"},
        {"name": "call_drop_rate", "unit": "%", "base": 0.5, "noise": 0.3, "type": "GAUGE"},
        {"name": "rrc_connection_success_rate", "unit": "%", "base": 99.2, "noise": 0.5, "type": "GAUGE"},
        {"name": "active_users", "unit": "count", "base": 120, "noise": 40, "type": "GAUGE", "diurnal": True},
        {"name": "cpu_pct", "unit": "%", "base": 35, "noise": 10, "type": "GAUGE", "diurnal": True},
        {"name": "temperature_c", "unit": "°C", "base": 42, "noise": 5, "type": "GAUGE"},
    ],
    "Router": [
        {"name": "cpu_pct", "unit": "%", "base": 30, "noise": 8, "type": "GAUGE", "diurnal": True},
        {"name": "memory_pct", "unit": "%", "base": 55, "noise": 10, "type": "GAUGE"},
        {"name": "bandwidth_utilization_pct", "unit": "%", "base": 40, "noise": 15, "type": "GAUGE", "diurnal": True},
        {"name": "latency_ms", "unit": "ms", "base": 5, "noise": 2, "type": "GAUGE"},
        {"name": "jitter_ms", "unit": "ms", "base": 1.5, "noise": 0.8, "type": "GAUGE"},
        {"name": "packet_loss_pct", "unit": "%", "base": 0.02, "noise": 0.01, "type": "GAUGE"},
        {"name": "interface_errors", "unit": "count", "base": 2, "noise": 3, "type": "COUNTER"},
        {"name": "bgp_flaps", "unit": "count", "base": 0, "noise": 0.1, "type": "COUNTER"},
        {"name": "temperature_c", "unit": "°C", "base": 38, "noise": 4, "type": "GAUGE"},
    ],
    "Switch": [
        {"name": "cpu_pct", "unit": "%", "base": 25, "noise": 8, "type": "GAUGE", "diurnal": True},
        {"name": "memory_pct", "unit": "%", "base": 45, "noise": 8, "type": "GAUGE"},
        {"name": "bandwidth_utilization_pct", "unit": "%", "base": 35, "noise": 12, "type": "GAUGE", "diurnal": True},
        {"name": "latency_ms", "unit": "ms", "base": 1, "noise": 0.5, "type": "GAUGE"},
        {"name": "packet_loss_pct", "unit": "%", "base": 0.01, "noise": 0.005, "type": "GAUGE"},
        {"name": "interface_errors", "unit": "count", "base": 1, "noise": 2, "type": "COUNTER"},
        {"name": "port_utilization_pct", "unit": "%", "base": 50, "noise": 15, "type": "GAUGE"},
        {"name": "temperature_c", "unit": "°C", "base": 35, "noise": 3, "type": "GAUGE"},
    ],
    "FiberLink": [
        {"name": "bandwidth_utilization_pct", "unit": "%", "base": 45, "noise": 15, "type": "GAUGE", "diurnal": True},
        {"name": "latency_ms", "unit": "ms", "base": 3, "noise": 1, "type": "GAUGE"},
        {"name": "jitter_ms", "unit": "ms", "base": 0.5, "noise": 0.3, "type": "GAUGE"},
        {"name": "packet_loss_pct", "unit": "%", "base": 0.005, "noise": 0.003, "type": "GAUGE"},
        {"name": "optical_power_dbm", "unit": "dBm", "base": -8, "noise": 1.5, "type": "GAUGE"},
        {"name": "ber", "unit": "rate", "base": 1e-12, "noise": 5e-13, "type": "GAUGE"},
        {"name": "error_count", "unit": "count", "base": 0, "noise": 1, "type": "COUNTER"},
    ],
    "CoreNode": [
        {"name": "cpu_pct", "unit": "%", "base": 40, "noise": 10, "type": "GAUGE", "diurnal": True},
        {"name": "memory_pct", "unit": "%", "base": 60, "noise": 8, "type": "GAUGE"},
        {"name": "throughput_gbps", "unit": "Gbps", "base": 25, "noise": 8, "type": "GAUGE", "diurnal": True},
        {"name": "latency_ms", "unit": "ms", "base": 2, "noise": 0.8, "type": "GAUGE"},
        {"name": "packet_loss_pct", "unit": "%", "base": 0.001, "noise": 0.001, "type": "GAUGE"},
        {"name": "active_sessions", "unit": "count", "base": 50000, "noise": 15000, "type": "GAUGE", "diurnal": True},
        {"name": "signaling_load_pct", "unit": "%", "base": 35, "noise": 12, "type": "GAUGE", "diurnal": True},
    ]
}

# COMMAND ----------

# Fault impact functions — how each fault type affects metrics
def apply_fault_impact(metric_name, base_value, fault_type, is_primary, progress):
    """
    Apply fault impact to a metric value.
    progress: 0.0 (fault start) to 1.0 (fault end)
    """
    if fault_type == "FIBER_CUT":
        if is_primary:
            if metric_name in ("bandwidth_utilization_pct", "throughput_dl_mbps", "throughput_ul_mbps", "throughput_gbps"):
                return base_value * 0.05  # Near-zero throughput
            if metric_name == "packet_loss_pct":
                return 95.0  # Massive packet loss
            if metric_name in ("latency_ms",):
                return base_value * 50  # Extreme latency
            if metric_name == "optical_power_dbm":
                return -40.0  # No optical signal
            if metric_name == "ber":
                return 1e-3  # Terrible BER
        else:  # Downstream impact
            if metric_name in ("latency_ms",):
                return base_value * 5
            if metric_name == "packet_loss_pct":
                return base_value * 100
            if metric_name in ("throughput_dl_mbps", "throughput_ul_mbps"):
                return base_value * 0.3

    elif fault_type == "TRANSCEIVER_DEGRADATION":
        # Gradual degradation — key for proactive detection
        if is_primary:
            if metric_name == "optical_power_dbm":
                return base_value - (15 * progress)  # Slowly drops
            if metric_name == "ber":
                return base_value * (1 + 1e8 * progress ** 2)  # Exponential BER rise
            if metric_name == "error_count":
                return base_value + 50 * progress ** 2
            if metric_name == "packet_loss_pct":
                return base_value + 2 * progress ** 2

    elif fault_type == "CELL_OVERLOAD":
        if is_primary:
            if metric_name == "active_users":
                return base_value * (3 + 2 * math.sin(progress * math.pi))
            if metric_name == "cpu_pct":
                return min(98, base_value * 2.5)
            if metric_name in ("throughput_dl_mbps", "throughput_ul_mbps"):
                return base_value * 0.3  # Congested
            if metric_name == "call_drop_rate":
                return min(15, base_value * 20)
            if metric_name == "rrc_connection_success_rate":
                return max(70, base_value - 25)
            if metric_name == "handover_success_rate":
                return max(75, base_value - 20)

    elif fault_type == "CPU_OVERLOAD":
        if is_primary:
            if metric_name == "cpu_pct":
                return min(99, 85 + 10 * math.sin(progress * math.pi * 4))
            if metric_name == "latency_ms":
                return base_value * 10
            if metric_name == "bgp_flaps":
                return random.randint(5, 20)
            if metric_name == "packet_loss_pct":
                return base_value * 50
        else:
            if metric_name == "latency_ms":
                return base_value * 3

    elif fault_type == "MEMORY_LEAK":
        if is_primary:
            if metric_name == "memory_pct":
                return min(98, 60 + 38 * progress)  # Steadily climbing
            if metric_name == "cpu_pct":
                return base_value * (1 + 0.5 * progress)
            if metric_name == "latency_ms":
                return base_value * (1 + 5 * progress ** 2)

    elif fault_type == "POWER_SUPPLY_FAILURE":
        if is_primary:
            # Abrupt — everything drops
            if metric_name in ("cpu_pct", "memory_pct", "bandwidth_utilization_pct", "port_utilization_pct"):
                return 0
            if metric_name == "temperature_c":
                return base_value - 20

    elif fault_type == "TEMPERATURE_HIGH":
        if is_primary:
            if metric_name == "temperature_c":
                return min(85, base_value + 30 * (0.5 + 0.5 * math.sin(progress * math.pi)))
            if metric_name == "cpu_pct":
                return min(95, base_value * 1.4)  # Thermal throttling

    elif fault_type == "LINK_FLAP":
        if is_primary:
            # Intermittent — alternates between good and bad
            flapping = math.sin(progress * math.pi * 20) > 0.3
            if metric_name == "packet_loss_pct":
                return 30.0 if flapping else base_value
            if metric_name == "latency_ms":
                return base_value * 20 if flapping else base_value
            if metric_name == "error_count":
                return 100 if flapping else base_value
            if metric_name == "ber":
                return 1e-5 if flapping else base_value

    elif fault_type == "CORE_CONGESTION":
        if is_primary:
            if metric_name == "cpu_pct":
                return min(99, base_value * 2.5)
            if metric_name == "signaling_load_pct":
                return min(99, base_value * 2.8)
            if metric_name == "latency_ms":
                return base_value * 15
            if metric_name == "packet_loss_pct":
                return base_value * 500
        else:
            if metric_name == "latency_ms":
                return base_value * 5
            if metric_name == "packet_loss_pct":
                return base_value * 50

    elif fault_type == "RADIO_DEGRADATION":
        if is_primary:
            if metric_name == "rsrp_dbm":
                return base_value - 15 * progress
            if metric_name == "rsrq_db":
                return base_value - 8 * progress
            if metric_name == "throughput_dl_mbps":
                return base_value * (1 - 0.5 * progress)
            if metric_name == "handover_success_rate":
                return max(80, base_value - 15 * progress)

    elif fault_type == "HANDOVER_FAILURE":
        if is_primary:
            if metric_name == "handover_success_rate":
                return max(50, base_value - 45)
            if metric_name == "call_drop_rate":
                return min(20, base_value * 30)
            if metric_name == "rrc_connection_success_rate":
                return max(75, base_value - 20)

    return None  # No impact

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate PM counter rows
# MAGIC We generate in batches by element type to manage memory, then write to Delta.

# COMMAND ----------

def generate_pm_for_element(element, timestamps):
    """Generate all PM counter rows for a single element."""
    eid = element["element_id"]
    etype = element["element_type"]
    metrics = METRIC_DEFS.get(etype, [])
    rows = []

    # Add some per-element variance
    element_seed = hash(eid) % 1000
    random.seed(element_seed)
    element_offsets = {m["name"]: random.gauss(0, m["noise"] * 0.3) for m in metrics}
    random.seed()

    for ts in timestamps:
        hour = ts.hour
        d_factor = diurnal_factor(hour)
        active_faults = get_active_faults(eid, ts)

        for metric in metrics:
            mname = metric["name"]
            base = metric["base"] + element_offsets[mname]

            # Apply diurnal pattern if applicable
            if metric.get("diurnal"):
                if "pct" in mname or "rate" in mname:
                    value = base * (0.6 + 0.4 * d_factor)
                else:
                    value = base * d_factor
            else:
                value = base

            # Add noise
            value += random.gauss(0, metric["noise"])

            # Apply fault impacts
            for fault, is_primary in active_faults:
                fault_start = fault["start"]
                fault_end = fault_start + timedelta(hours=fault["duration_h"])
                progress = (ts - fault_start).total_seconds() / (fault_end - fault_start).total_seconds()
                progress = max(0, min(1, progress))

                impacted = apply_fault_impact(mname, value, fault["type"], is_primary, progress)
                if impacted is not None:
                    value = impacted + random.gauss(0, metric["noise"] * 0.2)  # Small noise on fault values too

            # Clamp values
            if "pct" in mname or "rate" in mname:
                value = max(0, min(100, value))
            elif mname == "active_users" or mname == "active_sessions":
                value = max(0, int(value))
            elif mname == "interface_errors" or mname == "bgp_flaps" or mname == "error_count":
                value = max(0, round(value))
            elif "dbm" in mname:
                value = max(-50, min(0, value))
            elif mname == "temperature_c":
                value = max(15, min(90, value))
            elif mname == "ber":
                value = max(0, value)
            else:
                value = max(0, value)

            rows.append((eid, ts.isoformat(), mname, float(value), metric["unit"], metric["type"], START_DATE.isoformat()))

    return rows

# COMMAND ----------

# Generate timestamps (5-min intervals for 30 days)
timestamps = []
current = START_DATE
while current < END_DATE:
    timestamps.append(current)
    current += timedelta(minutes=INTERVAL_MINUTES)

print(f"Generating PM counters: {len(timestamps)} timestamps × {len(elements)} elements")
print(f"Time range: {timestamps[0]} to {timestamps[-1]}")

# COMMAND ----------

# Generate and write PM data in batches by element type to avoid OOM
pm_schema = StructType([
    StructField("element_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("metric_name", StringType()),
    StructField("metric_value", DoubleType()),
    StructField("unit", StringType()),
    StructField("collection_type", StringType()),
    StructField("ingested_at", StringType())
])

# Sample timestamps — use every 3rd for large element types to keep size manageable
# Full resolution for fiber/core (fewer elements), sampled for eNodeB (many elements)
total_rows = 0

for etype in ["CoreNode", "FiberLink", "Router", "Switch", "eNodeB"]:
    type_elements = elements_by_type.get(etype, [])
    if not type_elements:
        continue

    # Use fewer timestamps for element types with many elements
    if etype == "eNodeB" and len(type_elements) > 100:
        ts_sample = timestamps[::3]  # Every 15 minutes for cells
    elif etype == "Switch" and len(type_elements) > 50:
        ts_sample = timestamps[::2]  # Every 10 minutes for switches
    else:
        ts_sample = timestamps  # Full 5-minute resolution

    print(f"Generating {etype}: {len(type_elements)} elements × {len(ts_sample)} timestamps × {len(METRIC_DEFS.get(etype, []))} metrics...")

    all_rows = []
    for i, elem in enumerate(type_elements):
        rows = generate_pm_for_element(elem, ts_sample)
        all_rows.extend(rows)
        if (i + 1) % 50 == 0:
            print(f"  Processed {i+1}/{len(type_elements)} {etype} elements...")

    if all_rows:
        df = spark.createDataFrame(all_rows, schema=pm_schema)
        df = df.withColumn("timestamp", F.to_timestamp("timestamp")) \
               .withColumn("ingested_at", F.to_timestamp("ingested_at"))

        write_mode = "overwrite" if etype == "CoreNode" else "append"
        df.write.mode(write_mode).saveAsTable("bronze_pm_counters")

        count = len(all_rows)
        total_rows += count
        print(f"  ✓ Wrote {count:,} rows for {etype}")

print(f"\n✓ Total PM counter rows: {total_rows:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Alarm Generation
# MAGIC
# MAGIC Generate alarms that correspond to fault scenarios plus background noise alarms.

# COMMAND ----------

ALARM_TYPES = {
    "FIBER_CUT": ["LINK_DOWN", "LOS_ALARM", "HIGH_BER", "OPTICAL_POWER_LOW"],
    "TRANSCEIVER_DEGRADATION": ["HIGH_BER", "OPTICAL_POWER_LOW", "CRC_ERRORS_HIGH"],
    "CELL_OVERLOAD": ["HIGH_CPU", "CELL_CONGESTION", "HANDOVER_FAILURE_HIGH", "RRC_SETUP_FAILURE"],
    "CPU_OVERLOAD": ["HIGH_CPU", "BGP_PEER_DOWN", "PROCESS_RESTART", "ROUTING_INSTABILITY"],
    "MEMORY_LEAK": ["HIGH_MEMORY", "PROCESS_RESTART", "SERVICE_DEGRADED"],
    "POWER_SUPPLY_FAILURE": ["POWER_SUPPLY_FAIL", "EQUIPMENT_DOWN", "LINK_DOWN"],
    "TEMPERATURE_HIGH": ["TEMPERATURE_ALARM", "THERMAL_SHUTDOWN_WARNING", "FAN_FAILURE"],
    "LINK_FLAP": ["LINK_FLAP", "HIGH_BER", "CRC_ERRORS_HIGH", "INTERFACE_ERROR"],
    "CORE_CONGESTION": ["HIGH_CPU", "HIGH_MEMORY", "SIGNALING_OVERLOAD", "SESSION_LIMIT_REACHED"],
    "RADIO_DEGRADATION": ["LOW_RSRP", "POOR_RSRQ", "ANTENNA_ALARM", "VSWR_HIGH"],
    "HANDOVER_FAILURE": ["HANDOVER_FAILURE_HIGH", "X2_LINK_FAILURE", "S1_SETUP_FAILURE"],
}

SEVERITY_MAP = {
    "LINK_DOWN": "CRITICAL", "LOS_ALARM": "CRITICAL", "EQUIPMENT_DOWN": "CRITICAL",
    "POWER_SUPPLY_FAIL": "CRITICAL", "BGP_PEER_DOWN": "CRITICAL", "THERMAL_SHUTDOWN_WARNING": "CRITICAL",
    "SIGNALING_OVERLOAD": "CRITICAL", "SESSION_LIMIT_REACHED": "CRITICAL",
    "HIGH_BER": "MAJOR", "OPTICAL_POWER_LOW": "MAJOR", "HIGH_CPU": "MAJOR",
    "HIGH_MEMORY": "MAJOR", "CELL_CONGESTION": "MAJOR", "HANDOVER_FAILURE_HIGH": "MAJOR",
    "ROUTING_INSTABILITY": "MAJOR", "LINK_FLAP": "MAJOR", "ANTENNA_ALARM": "MAJOR",
    "X2_LINK_FAILURE": "MAJOR", "S1_SETUP_FAILURE": "MAJOR",
    "CRC_ERRORS_HIGH": "MINOR", "RRC_SETUP_FAILURE": "MINOR", "PROCESS_RESTART": "MINOR",
    "SERVICE_DEGRADED": "MINOR", "FAN_FAILURE": "MINOR", "INTERFACE_ERROR": "MINOR",
    "LOW_RSRP": "MINOR", "POOR_RSRQ": "MINOR", "VSWR_HIGH": "MINOR",
    "TEMPERATURE_ALARM": "WARNING",
}

ALARM_DESCRIPTIONS = {
    "LINK_DOWN": "Physical link down detected — no signal received",
    "LOS_ALARM": "Loss of Signal on optical interface",
    "HIGH_BER": "Bit Error Rate exceeds threshold (>{threshold})",
    "OPTICAL_POWER_LOW": "Received optical power below minimum threshold",
    "CRC_ERRORS_HIGH": "CRC error rate exceeding normal levels",
    "HIGH_CPU": "CPU utilization exceeded {threshold}% for sustained period",
    "HIGH_MEMORY": "Memory utilization exceeded {threshold}%",
    "CELL_CONGESTION": "Cell congestion — active users exceed capacity",
    "HANDOVER_FAILURE_HIGH": "Handover failure rate exceeds {threshold}%",
    "RRC_SETUP_FAILURE": "RRC connection setup failure rate elevated",
    "BGP_PEER_DOWN": "BGP peer session lost with neighbor",
    "PROCESS_RESTART": "Critical process restarted unexpectedly",
    "ROUTING_INSTABILITY": "Multiple route changes detected — routing table unstable",
    "POWER_SUPPLY_FAIL": "Power supply unit failure detected",
    "EQUIPMENT_DOWN": "Network element unreachable — equipment down",
    "TEMPERATURE_ALARM": "Equipment temperature above warning threshold",
    "THERMAL_SHUTDOWN_WARNING": "Equipment approaching thermal shutdown temperature",
    "FAN_FAILURE": "Cooling fan failure detected",
    "LINK_FLAP": "Link state oscillating — up/down cycles detected",
    "INTERFACE_ERROR": "Interface error counters incrementing rapidly",
    "SIGNALING_OVERLOAD": "Signaling processing capacity exceeded",
    "SESSION_LIMIT_REACHED": "Maximum session count reached on node",
    "SERVICE_DEGRADED": "Service performance below acceptable thresholds",
    "LOW_RSRP": "Reference Signal Received Power below minimum",
    "POOR_RSRQ": "Reference Signal Received Quality degraded",
    "ANTENNA_ALARM": "Antenna system fault detected",
    "VSWR_HIGH": "Voltage Standing Wave Ratio exceeds threshold",
    "X2_LINK_FAILURE": "X2 interface link to neighbor eNodeB failed",
    "S1_SETUP_FAILURE": "S1 interface setup to MME failed",
}

# COMMAND ----------

alarms = []

# Generate fault-related alarms
for fault in FAULT_SCENARIOS:
    fid = fault["id"]
    if fid not in fault_elements:
        continue

    alarm_types = ALARM_TYPES.get(fault["type"], ["GENERIC_ALARM"])
    fault_start = fault["start"]
    fault_duration = timedelta(hours=fault["duration_h"])

    # Primary elements get more alarms
    for eid in fault_elements[fid]["primary"]:
        # Initial alarm burst at fault start
        for atype in alarm_types:
            alarm_time = fault_start + timedelta(seconds=random.randint(0, 120))
            clear_time = fault_start + fault_duration + timedelta(minutes=random.randint(-30, 30))
            clear_time = max(alarm_time + timedelta(minutes=5), clear_time)

            alarms.append({
                "alarm_id": str(uuid.uuid4())[:12],
                "element_id": eid,
                "alarm_time": alarm_time.isoformat(),
                "clear_time": clear_time.isoformat(),
                "severity": SEVERITY_MAP.get(atype, "MINOR"),
                "alarm_type": atype,
                "alarm_text": ALARM_DESCRIPTIONS.get(atype, f"Alarm: {atype}").format(threshold=random.randint(80, 95)),
                "probable_cause": fault["type"],
                "is_active": False,
                "acknowledged": True,
                "acknowledged_by": random.choice(["NOC_operator_1", "NOC_operator_2", "auto_ack"]),
                "ingested_at": alarm_time.isoformat(),
                "fault_id": fid  # For tracking
            })

            # For link flapping, generate repeated alarms
            if fault["type"] == "LINK_FLAP":
                n_flaps = random.randint(15, 40)
                for j in range(n_flaps):
                    flap_time = fault_start + timedelta(minutes=random.randint(0, int(fault["duration_h"] * 60)))
                    flap_clear = flap_time + timedelta(minutes=random.randint(1, 10))
                    alarms.append({
                        "alarm_id": str(uuid.uuid4())[:12],
                        "element_id": eid,
                        "alarm_time": flap_time.isoformat(),
                        "clear_time": flap_clear.isoformat(),
                        "severity": "MAJOR" if j % 3 == 0 else "MINOR",
                        "alarm_type": random.choice(["LINK_FLAP", "INTERFACE_ERROR"]),
                        "alarm_text": f"Link flap #{j+1} — interface oscillating",
                        "probable_cause": "LINK_FLAP",
                        "is_active": False,
                        "acknowledged": random.random() > 0.3,
                        "acknowledged_by": random.choice(["NOC_operator_1", "NOC_operator_2", "auto_ack"]) if random.random() > 0.3 else None,
                        "ingested_at": flap_time.isoformat(),
                        "fault_id": fid
                    })

    # Downstream elements get fewer, delayed alarms
    for eid in fault_elements[fid]["downstream"][:20]:  # Cap at 20 downstream
        n_downstream_alarms = random.randint(1, 3)
        for _ in range(n_downstream_alarms):
            atype = random.choice(["SERVICE_DEGRADED", "HIGH_CPU", "INTERFACE_ERROR", "LINK_DOWN"])
            delay = timedelta(seconds=random.randint(30, 600))
            alarm_time = fault_start + delay
            clear_time = fault_start + fault_duration + timedelta(minutes=random.randint(0, 60))

            alarms.append({
                "alarm_id": str(uuid.uuid4())[:12],
                "element_id": eid,
                "alarm_time": alarm_time.isoformat(),
                "clear_time": max(alarm_time + timedelta(minutes=5), clear_time).isoformat(),
                "severity": random.choice(["MAJOR", "MINOR", "WARNING"]),
                "alarm_type": atype,
                "alarm_text": f"Secondary impact — {ALARM_DESCRIPTIONS.get(atype, atype).format(threshold=random.randint(70, 90))}",
                "probable_cause": fault["type"],
                "is_active": False,
                "acknowledged": random.random() > 0.5,
                "acknowledged_by": random.choice(["NOC_operator_1", "NOC_operator_2"]) if random.random() > 0.5 else None,
                "ingested_at": alarm_time.isoformat(),
                "fault_id": fid
            })

# Background noise alarms (non-fault, normal operational noise)
background_alarm_types = ["TEMPERATURE_ALARM", "FAN_FAILURE", "INTERFACE_ERROR", "CRC_ERRORS_HIGH", "PROCESS_RESTART"]
for _ in range(500):
    elem = random.choice(elements)
    atype = random.choice(background_alarm_types)
    alarm_time = START_DATE + timedelta(seconds=random.randint(0, int((END_DATE - START_DATE).total_seconds())))
    duration = timedelta(minutes=random.randint(5, 120))

    alarms.append({
        "alarm_id": str(uuid.uuid4())[:12],
        "element_id": elem["element_id"],
        "alarm_time": alarm_time.isoformat(),
        "clear_time": (alarm_time + duration).isoformat(),
        "severity": random.choice(["WARNING", "MINOR", "MINOR", "MINOR"]),
        "alarm_type": atype,
        "alarm_text": ALARM_DESCRIPTIONS.get(atype, atype).format(threshold=random.randint(70, 90)),
        "probable_cause": "NORMAL_OPERATION",
        "is_active": False,
        "acknowledged": True,
        "acknowledged_by": "auto_ack",
        "ingested_at": alarm_time.isoformat(),
        "fault_id": None
    })

print(f"Generated {len(alarms)} total alarms")
severity_counts = {}
for a in alarms:
    severity_counts[a["severity"]] = severity_counts.get(a["severity"], 0) + 1
for s, c in sorted(severity_counts.items()):
    print(f"  {s}: {c}")

# COMMAND ----------

# Write alarms to bronze table
alarm_rows = []
for a in alarms:
    alarm_rows.append(Row(
        alarm_id=a["alarm_id"], element_id=a["element_id"],
        alarm_time=a["alarm_time"], clear_time=a["clear_time"],
        severity=a["severity"], alarm_type=a["alarm_type"],
        alarm_text=a["alarm_text"], probable_cause=a["probable_cause"],
        is_active=a["is_active"], acknowledged=a["acknowledged"],
        acknowledged_by=a.get("acknowledged_by"), ingested_at=a["ingested_at"]
    ))

alarm_df = spark.createDataFrame(alarm_rows)
alarm_df = alarm_df.withColumn("alarm_time", F.to_timestamp("alarm_time")) \
                   .withColumn("clear_time", F.to_timestamp("clear_time")) \
                   .withColumn("ingested_at", F.to_timestamp("ingested_at"))

alarm_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_alarms")
print(f"✓ Wrote {alarm_df.count()} alarms to bronze_alarms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Trouble Tickets Generation
# MAGIC
# MAGIC Historical incidents corresponding to the fault scenarios, with root cause labels.

# COMMAND ----------

tickets = []
for fault in FAULT_SCENARIOS:
    fid = fault["id"]
    if fid not in fault_elements:
        continue

    # P1 for CRITICAL, P2 for MAJOR, P3 for MINOR
    priority = {"CRITICAL": "P1", "MAJOR": "P2", "MINOR": "P3"}.get(fault["severity"], "P3")

    # Category mapping
    category_map = {
        "FIBER_CUT": "Fiber", "TRANSCEIVER_DEGRADATION": "Hardware",
        "CELL_OVERLOAD": "Capacity", "CPU_OVERLOAD": "Software",
        "MEMORY_LEAK": "Software", "POWER_SUPPLY_FAILURE": "Hardware",
        "TEMPERATURE_HIGH": "Power", "LINK_FLAP": "Fiber",
        "CORE_CONGESTION": "Capacity", "RADIO_DEGRADATION": "Hardware",
        "HANDOVER_FAILURE": "Software"
    }

    resolution_map = {
        "FIBER_CUT": "Emergency fiber splice team dispatched. Break repaired and link restored.",
        "TRANSCEIVER_DEGRADATION": "Transceiver module replaced with spare. Optical levels normalized.",
        "CELL_OVERLOAD": "Temporary capacity expansion via mobile cell unit. Traffic normalized post-event.",
        "CPU_OVERLOAD": "Root cause: BGP route leak from peer. Prefix filters applied. CPU stabilized.",
        "MEMORY_LEAK": "Software patched to version 12.4.3 fixing memory leak in session handler.",
        "POWER_SUPPLY_FAILURE": "Failed PSU replaced. Redundant PSU configuration verified.",
        "TEMPERATURE_HIGH": "AC unit repaired. Additional ventilation installed at site.",
        "LINK_FLAP": "Loose fiber connector at patch panel re-terminated. Link stabilized.",
        "CORE_CONGESTION": "DDoS mitigation filters applied. Traffic scrubbing enabled. Normal after 3h.",
        "RADIO_DEGRADATION": "Antenna tilt corrected from 8° back to design spec 4°. RSRP recovered.",
        "HANDOVER_FAILURE": "Software rollback to previous version. Vendor notified of bug in 5.2.1.",
    }

    affected = fault_elements[fid]["all_affected"]
    customers_per_element = {"eNodeB": 200, "Router": 5000, "Switch": 2000, "FiberLink": 3000, "CoreNode": 50000}
    est_customers = len(affected) * customers_per_element.get(fault["element_type"], 1000)

    # MTTR varies by severity and type
    base_mttr = {"FIBER_CUT": 240, "TRANSCEIVER_DEGRADATION": 120, "CELL_OVERLOAD": 60,
                 "CPU_OVERLOAD": 45, "MEMORY_LEAK": 180, "POWER_SUPPLY_FAILURE": 90,
                 "TEMPERATURE_HIGH": 120, "LINK_FLAP": 60, "CORE_CONGESTION": 90,
                 "RADIO_DEGRADATION": 30, "HANDOVER_FAILURE": 45}
    mttr = base_mttr.get(fault["type"], 60) + random.randint(-20, 40)

    tickets.append(Row(
        ticket_id=f"TT-{fid}-{random.randint(10000,99999)}",
        created_at=fault["start"].isoformat(),
        resolved_at=(fault["start"] + timedelta(minutes=mttr)).isoformat(),
        priority=priority,
        category=category_map.get(fault["type"], "Other"),
        root_cause=fault["type"],
        affected_elements=",".join(affected[:10]),
        affected_region=fault["region"],
        affected_customers=min(est_customers, 100000),
        resolution_notes=resolution_map.get(fault["type"], "Issue resolved."),
        mttr_minutes=mttr
    ))

ticket_schema = StructType([
    StructField("ticket_id", StringType()), StructField("created_at", StringType()),
    StructField("resolved_at", StringType()), StructField("priority", StringType()),
    StructField("category", StringType()), StructField("root_cause", StringType()),
    StructField("affected_elements", StringType()), StructField("affected_region", StringType()),
    StructField("affected_customers", IntegerType()), StructField("resolution_notes", StringType()),
    StructField("mttr_minutes", IntegerType())
])
ticket_df = spark.createDataFrame(tickets, schema=ticket_schema)
ticket_df = ticket_df.withColumn("created_at", F.to_timestamp("created_at")) \
                     .withColumn("resolved_at", F.to_timestamp("resolved_at"))
ticket_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_trouble_tickets")
print(f"✓ Wrote {ticket_df.count()} trouble tickets to bronze_trouble_tickets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation Summary

# COMMAND ----------

print("=" * 60)
print("DATA GENERATION COMPLETE")
print("=" * 60)
for table in ["bronze_network_topology", "bronze_pm_counters", "bronze_alarms", "bronze_trouble_tickets"]:
    count = spark.table(table).count()
    print(f"  {table}: {count:,} rows")
print("=" * 60)
print("\nNext: Run 02_medallion_pipeline to process Bronze → Silver → Gold")

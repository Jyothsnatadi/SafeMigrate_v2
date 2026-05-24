---
name: best-practices-and-cost-review
description: Cost efficiency and best practices audit for Databricks pipelines. Covers both SDP (Spark Declarative Pipelines) and PySpark job workloads. Reviews table storage, incrementalisation opportunities, compute configuration, cluster sizing, and maintenance. Produces a per-table recommendation report.
user-invocable: true
---

# Databricks Pipeline Cost & Best Practices Review

This skill audits Databricks pipelines for cost efficiency, incrementalisation opportunities, table storage best practices, and compute configuration. It covers both **SDP (Spark Declarative Pipelines)** and **PySpark jobs running on Databricks Workflows**.

**Announce at start:** "I'm using the best-practices-and-cost-review skill to audit your pipeline for cost efficiency and Databricks best practices."

## Overview: The Cost Review Framework

```
1. SETUP              -> Identify the pipeline, tables, and current configuration
2. TABLE STORAGE      -> Audit file layout, table properties, clustering
3. INCREMENTALISATION -> Classify each table's refresh pattern and recommend incremental strategy
4. COMPUTE            -> Review pipeline edition, serverless config, Photon
5. MAINTENANCE        -> Check VACUUM, OPTIMIZE, ANALYZE TABLE status
6. COST ANALYSIS      -> Estimate current Databricks cost and savings opportunities
7. REPORT             -> Per-table recommendations with estimated savings
```

**Reference files:**
- `reference/cost-and-best-practices.md` — Full checklist, anti-patterns, and cost estimation queries

---

## Step 1: Setup

### Gather Required Information

Ask the user for the following:

| Input | Description | Example |
|---|---|---|
| **Workload type** | Is this an SDP pipeline or a PySpark job (or both)? | `SDP`, `PySpark job`, `both` |
| **Catalog** | Databricks catalog where data lands | `avi_insighthub_d_curated_reg` |
| **Schema** | Schema containing the tables | `conform_main_aviation` |
| **Table(s)** | Tables to audit (or "all" to discover) | `l3_mip_gd_contracts, l4_ops_mi_mip_waterfall` |
| **Pipeline/Job name** | SDP pipeline name or Workflow job name | `aviation_ih_ops_mi_layer_34` |
| **Business domain** | For locating code in the repo | `aviation`, `midstream`, `castrol` |

### Identify the Workload Type

The recommendations differ depending on the workload type:

| Workload Type | What It Is | Where to Find Config |
|---|---|---|
| **SDP (Spark Declarative Pipelines)** | Declarative pipeline using `@dlt.table` / `@dlt.view` decorators. Managed by SDP runtime. | `resources/*_dlt_bundle.yml` in the bundle config |
| **PySpark Job** | Imperative PySpark script running as a Databricks Workflow task. Uses `spark.read` / `spark.write` directly. | `resources/*_job_bundle.yml` or job definition in the Workflows UI |
| **Both** | Some tables produced by SDP, others by PySpark jobs in the same orchestration workflow | Check the job definition for mixed task types |

If unsure, look at the code:
- `import dlt` → SDP pipeline
- `spark.read.table()` + `df.write.mode("overwrite").saveAsTable()` → PySpark job
- Both can coexist in the same Databricks Bundle / Workflow

### Discover Tables (if not specified)

```sql
SHOW TABLES IN {catalog}.{schema}
```

### Baseline Each Table

For every table under review, collect the baseline:

```sql
DESCRIBE DETAIL {catalog}.{schema}.{table}
```

Record: `numFiles`, `sizeInBytes`, `partitionColumns`, `clusteringColumns`, `format`.

---

## Step 2: Table Storage Audit

### 2a. Table Properties

```sql
SHOW TBLPROPERTIES {catalog}.{schema}.{table}
```

Check against the required Delta table properties — see `reference/cost-and-best-practices.md` §2 for the full checklist and rationale.

For each missing property, generate the ALTER TABLE statement:

```sql
ALTER TABLE {catalog}.{schema}.{table}
SET TBLPROPERTIES (
  delta.enableChangeDataFeed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true,
  delta.enableDeletionVectors = true,
  delta.enableRowTracking = true
);
```

### 2b. Small File Detection

```sql
SELECT
  numFiles,
  ROUND(sizeInBytes / (1024*1024*1024), 2) AS size_gb,
  CASE WHEN numFiles > 0 THEN ROUND(sizeInBytes / numFiles / (1024*1024), 1) ELSE 0 END AS avg_file_size_mb
FROM (DESCRIBE DETAIL {catalog}.{schema}.{table})
```

| Avg File Size | Verdict |
|---|---|
| > 128 MB | Good |
| 32-128 MB | Acceptable |
| < 32 MB | **Small file problem** — enable autoOptimize, run OPTIMIZE |
| > 1 GB | Potentially too large for filtered reads — check clustering |

### 2c. Liquid Clustering Assessment

```sql
-- Check current clustering
DESCRIBE DETAIL {catalog}.{schema}.{table};
```

If the table is a candidate for liquid clustering (see `reference/cost-and-best-practices.md` §2 for when to apply vs avoid, and how to pick clustering columns):

```sql
ALTER TABLE {catalog}.{schema}.{table}
CLUSTER BY ({recommended_col_1}, {recommended_col_2});
```

---

## Step 3: Incrementalisation

**This is the single biggest cost lever. Incrementalisation should be the default, not the exception.**

Both SDP and PySpark on Databricks provide incremental processing patterns. The question is not "should we incrementalise?" — it's "what's the right incremental pattern?"

### Classify Each Table

Read the pipeline code and classify each table's current refresh pattern:

#### SDP Pipelines

| Current Pattern | Cost Impact | Recommendation |
|---|---|---|
| **Materialized View** (full recompute) | HIGH for large tables | **Convert to Streaming Table** if source supports CDC or append. Only keep full recompute for small reference/dimension tables (< 1GB). |
| **Streaming Table** (incremental append) | LOW | Already optimal. Verify watermarking is configured. |
| **Materialized View with incremental hints** | MEDIUM | Acceptable for tables needing idempotent correctness + incremental efficiency. |

#### PySpark Jobs

| Current Pattern | Cost Impact | Recommendation |
|---|---|---|
| **Full overwrite** (`df.write.mode("overwrite").saveAsTable()`) | HIGH for large tables | **Convert to MERGE or partition-level overwrite** (see below). Only acceptable for small tables (< 1GB). |
| **Append only** (`df.write.mode("append").saveAsTable()`) | LOW | Good for event/log data. Verify dedup logic exists if source can replay. |
| **Manual HWM tracking** (external store, Delta table, widget param) | MEDIUM-HIGH | **Replace with Delta CDF `readStream` or MERGE with watermark column.** Manual HWM tracking is redundant with native Databricks features. |
| **Partition overwrite** (`df.write.mode("overwrite").partitionBy(...).option("replaceWhere", ...)`) | MEDIUM | Better than full overwrite. Consider MERGE if updates span partitions. |
| **MERGE INTO** (upsert) | LOW-MEDIUM | Good pattern. Check merge predicate selectivity — broad predicates rewrite too many files. |

### Decision Tree

```
Is the source table append-only or CDC-enabled?
├── YES
│   ├── SDP? -> Use Streaming Table (incremental append)
│   │          Enable CDF on source if not already
│   │
│   └── PySpark? -> Use readStream + foreachBatch for micro-batch
│                   OR append mode with dedup downstream
│
└── NO -> Is the table small (< 1GB)?
    ├── YES -> Full overwrite / Materialized View is fine
    │
    └── NO -> Can you identify changed rows (updated_at, partition_date, HWM)?
        ├── YES
        │   ├── SDP? -> Use Streaming Table with readStream + watermark
        │   │
        │   └── PySpark? -> Use MERGE INTO with watermark column:
        │                   1. Read only rows WHERE updated_at > last_run
        │                   2. MERGE INTO target ON business_key
        │                   3. Store last_run as job parameter or Delta checkpoint
        │
        └── NO -> Full recompute (flag as cost risk, recommend source-side CDF)
```

### Enabling CDF on Source Tables

```sql
ALTER TABLE {source_catalog}.{schema}.{table}
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

### Converting SDP to Streaming Table

```python
# BEFORE: Full read (materialized view)
@dlt.table(name="my_target")
def my_target():
    return spark.read.table("source_table")

# AFTER: Incremental read (streaming table)
@dlt.table(name="my_target")
def my_target():
    return spark.readStream.table("source_table")
```

First run processes all existing data (full backfill). Subsequent runs process only changes.

### Converting PySpark Jobs to Incremental

#### Pattern 1: Full Overwrite → MERGE INTO

```python
# BEFORE: Full overwrite (reads and rewrites entire table every run)
source_df = spark.read.table("source_catalog.schema.source_table")
transformed = source_df.filter(...).withColumn(...)
transformed.write.mode("overwrite").saveAsTable("target_catalog.schema.target_table")

# AFTER: Incremental MERGE (reads only changed rows, upserts into target)
from delta.tables import DeltaTable

# Read only new/changed rows since last run
last_watermark = spark.sql("SELECT MAX(updated_at) FROM target_catalog.schema.target_table").collect()[0][0]
incremental_df = spark.read.table("source_catalog.schema.source_table") \
    .filter(F.col("updated_at") > last_watermark)

transformed = incremental_df.filter(...).withColumn(...)

# Upsert into target
target = DeltaTable.forName(spark, "target_catalog.schema.target_table")
target.alias("t").merge(
    transformed.alias("s"),
    "t.business_key = s.business_key"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

#### Pattern 2: Full Overwrite → Partition-Level Overwrite

```python
# BEFORE: Overwrites entire table
transformed.write.mode("overwrite").saveAsTable("target")

# AFTER: Overwrites only the partition(s) being processed
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
transformed.write.mode("overwrite") \
    .partitionBy("partition_date") \
    .saveAsTable("target")
```

#### Pattern 3: Full Overwrite → Structured Streaming with foreachBatch

```python
# For PySpark jobs that need incremental processing with complex logic
def process_batch(batch_df, batch_id):
    transformed = batch_df.filter(...).withColumn(...)
    target = DeltaTable.forName(spark, "target_catalog.schema.target_table")
    target.alias("t").merge(
        transformed.alias("s"),
        "t.business_key = s.business_key"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

spark.readStream.table("source_catalog.schema.source_table") \
    .writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/checkpoints/my_pipeline") \
    .trigger(availableNow=True) \
    .start()
```

The `trigger(availableNow=True)` processes all available data then stops — behaves like a batch job but with streaming's incremental checkpoint tracking.

### Red Flags

- **Any table > 10GB using full overwrite or full recompute** — money on fire
- **Streaming tables without watermarking** — unbounded state, eventual OOM
- **PySpark jobs re-reading entire source table every run** — add watermark filter
- **History load notebooks running full table scans on every execution** — should be one-time backfill
- **`spark.table()` or `spark.read.table()` without partition pruning** — reads entire source
- **Manual HWM tracking (external store, Delta table, widget params)** — redundant with native Databricks features, replace with Delta CDF or MERGE with watermark
- **`df.write.mode("overwrite").saveAsTable()` on large tables** — rewrites all data even if only 1% changed

---

## Step 4: Compute Configuration

The compute review differs depending on whether the pipeline is SDP or a PySpark job.

### 4a. SDP Pipeline Config

Find the bundle config at `{domain}/databricks_pipeline/*/{pipeline}/resources/*_dlt_bundle.yml`.

| Setting | What to Check | Recommendation |
|---|---|---|
| `serverless: true` | Is serverless enabled? | **Default choice for all new pipelines** — no cluster management, auto-scaling, pay per DBU |
| `edition` | `CORE`, `PRO`, or `ADVANCED` | Use `CORE` unless you need CDC propagation (PRO) or enhanced autoscaling (ADVANCED). ADVANCED costs ~2x CORE. |
| `channel` | `CURRENT` or `PREVIEW` | Use `CURRENT` for production stability |
| `photon` | Is Photon enabled? | Should be `true` for SQL-heavy transformations — 2-8x speedup at same DBU cost |
| Cluster config (if not serverless) | Instance type, min/max workers | Check Spark UI utilisation — right-size or switch to serverless |

### 4b. PySpark Job Cluster Config

Find the job config at `{domain}/databricks_pipeline/*/{pipeline}/resources/*_job_bundle.yml` or in the Workflows UI.

| Setting | What to Check | Recommendation |
|---|---|---|
| **Cluster type** | Job cluster vs all-purpose cluster | **Always use job clusters** for production jobs — they spin up for the job and terminate after. All-purpose clusters charge for idle time. |
| **Serverless compute** | Is the job using serverless? | Preferred for most workloads — instant start, no cluster config, auto-right-sizing |
| **Instance type** | Over-provisioned? Memory-optimised when not needed? | Match instance type to workload: memory-optimised for large joins/caches, compute-optimised for transformations |
| **Autoscaling** | `min_workers` / `max_workers` | Set `min_workers` to the steady-state need, `max_workers` for peak. Avoid fixed-size clusters unless workload is predictable. |
| **Spot instances** | Are spot/preemptible instances enabled? | **Enable for worker nodes** — 60-90% cost savings. Keep driver on on-demand. |
| **Photon** | Enabled on the cluster? | Should be enabled for SQL-heavy and DataFrame-heavy workloads |
| **Auto-termination** | Is idle auto-termination configured? | Set to 10-15 minutes for job clusters. Not applicable if using serverless. |
| **Init scripts** | Are there expensive init scripts? | Review for unnecessary pip installs, conda setups, or large file downloads that add startup time and cost |

#### PySpark Job Cluster Sizing Guide

```
Data volume per run:
├── < 10 GB    -> 1-2 workers (or serverless)
├── 10-100 GB  -> 2-8 workers with autoscale
├── 100 GB-1TB -> 8-32 workers with autoscale + spot
└── > 1 TB     -> 32+ workers, consider partitioned approach
                  or break into multiple jobs
```

#### Check Actual Utilisation

```sql
-- Job run history with duration and cluster info
SELECT
  job_id,
  run_name,
  result_state,
  ROUND((end_time - start_time) / 60000, 1) AS duration_minutes,
  cluster_spec
FROM system.lakeflow.job_run_timeline
WHERE job_id = {job_id}
ORDER BY start_time DESC
LIMIT 20
```

Look for:
- **Runs consistently finishing in < 5 minutes** on a large cluster → over-provisioned, shrink or go serverless
- **Duration varies wildly** between runs → good candidate for autoscaling
- **Long idle/startup time** relative to actual compute → switch to serverless

### 4c. Serverless vs Classic (Both SDP and PySpark)

| Aspect | Serverless | Classic (Job Clusters) |
|---|---|---|
| Cost model | Pay per DBU consumed only | Pay for cluster uptime (idle + active) |
| Scaling | Automatic, instant, right-sized | Manual autoscaling with lag |
| Startup time | Near-instant | 2-5 minutes for new clusters |
| Management | Zero cluster config | Must choose instance types, worker counts |
| Custom libraries | Supported via Workflows | Full flexibility (init scripts, custom images) |
| Spark configs | Limited set supported | Full control |
| Recommendation | **Default for new pipelines** | When you need specific Spark configs, custom Docker images, GPU instances, or init scripts |

### 4d. Workflow Orchestration Review

For multi-task Workflows (common when a pipeline has both SDP and PySpark tasks):

| Setting | What to Check | Recommendation |
|---|---|---|
| **Shared job clusters** | Are multiple tasks sharing the same cluster? | Good for tasks that run sequentially and have similar resource needs. Avoid for parallel tasks with different profiles. |
| **Task dependencies** | Are tasks sequenced correctly? | Unnecessary sequential execution adds wall-clock time. Parallelise independent tasks. |
| **Retry policies** | Are retries configured? | Set max retries to 1-2 for transient failures. Unlimited retries on broken jobs = infinite cost. |
| **Timeout** | Is there a job timeout? | Always set a timeout. A stuck job with no timeout runs (and charges) until manually killed. |
| **Schedule** | Is the schedule appropriate? | Hourly schedules on data that changes daily = 23 wasted runs per day. Match schedule to source update frequency. |

---

## Step 5: Maintenance

### Check Maintenance History

```sql
DESCRIBE HISTORY {catalog}.{schema}.{table} LIMIT 20
```

Look for recent `OPTIMIZE`, `VACUUM`, and `ANALYZE` operations.

| Maintenance | Expected | Why |
|---|---|---|
| `OPTIMIZE` | Handled automatically if `autoOptimize` is on | Compacts small files |
| `VACUUM` | Should run periodically (7-day default retention) | Removes old file versions, reduces storage cost |
| `ANALYZE TABLE` | After major data loads | Updates statistics for query optimizer |

For VACUUM commands and retention configuration, see `reference/cost-and-best-practices.md` §4.

---

## Step 6: Cost Analysis

Help the user understand the current Databricks cost for the pipeline and where the optimisation opportunities lie.

### Gather Pipeline Costs

For **SDP pipelines:**
```sql
SELECT
  pipeline_name,
  update_id,
  state,
  TIMESTAMPDIFF(MINUTE, creation_time, last_update_time) AS duration_minutes
FROM system.lakeflow.pipeline_events
WHERE pipeline_name LIKE '%{pipeline_name}%'
  AND event_type = 'update_progress'
ORDER BY creation_time DESC
LIMIT 20
```

For **PySpark jobs:**
```sql
SELECT
  job_id,
  run_name,
  result_state,
  ROUND((end_time - start_time) / 60000, 1) AS duration_minutes
FROM system.lakeflow.job_run_timeline
WHERE run_name LIKE '%{job_name}%'
ORDER BY start_time DESC
LIMIT 20
```

For **DBU consumption** (both workload types):
```sql
SELECT
  workspace_id,
  sku_name,
  usage_date,
  SUM(usage_quantity) AS total_dbus
FROM system.billing.usage
WHERE usage_metadata.job_id = '{job_id}'
  OR usage_metadata.pipeline_id = '{pipeline_id}'
GROUP BY 1, 2, 3
ORDER BY usage_date DESC
LIMIT 30
```

For the Databricks cost components breakdown, see `reference/cost-and-best-practices.md` §7.

### Key Savings Levers

1. **Incrementalisation** — 80-90% compute reduction on large tables
2. **Serverless** — no idle cluster costs
3. **Photon** — 2-8x speedup at same DBU cost
4. **Native HWM via streaming tables** — removes operational overhead of external state stores
5. **Liquid clustering** — reduces scan cost for filtered queries

---

## Step 7: Report

Generate a per-table recommendation report:

```
Databricks Pipeline Cost & Best Practices Report
=================================================
Date:           {today}
Pipeline:       {pipeline_name}
Catalog/Schema: {catalog}.{schema}
Tables audited: {N}

PER-TABLE FINDINGS
------------------

Table: {table_name}
  Workload type: {SDP / PySpark Job}
  Size: {X} GB, {Y} files, avg {Z} MB/file
  Current pattern: {Materialized View / Streaming Table / Full Overwrite / MERGE / Manual HWM}
  Table properties: {N missing — list them}
  Clustering: {None / Hive partitioned / Liquid clustered by ...}
  Incrementalisation: {Already incremental / RECOMMEND: convert to streaming table / RECOMMEND: convert to MERGE}
  Estimated cost reduction: {X%}
  Actions:
    - ALTER TABLE ... SET TBLPROPERTIES (...)
    - ALTER TABLE ... CLUSTER BY (...)
    - SDP: Convert to readStream in pipeline code
    - PySpark: Convert write.mode("overwrite") to MERGE INTO
    - Enable CDF on source: ALTER TABLE {source} SET TBLPROPERTIES (...)

{Repeat for each table}

PIPELINE/JOB-LEVEL FINDINGS
---------------------------
  Workload type: {SDP / PySpark Job / Mixed Workflow}
  Edition (SDP): {CORE/PRO/ADVANCED} — {appropriate / recommend downgrade}
  Cluster type (PySpark): {Serverless / Job cluster / All-purpose} — {appropriate / recommend change}
  Serverless: {yes / RECOMMEND: switch to serverless}
  Photon: {enabled / RECOMMEND: enable}
  Spot instances (PySpark): {enabled / RECOMMEND: enable for workers}
  Autoscaling: {configured / RECOMMEND: add autoscaling}
  Timeout: {set / RECOMMEND: add timeout}
  Schedule vs data frequency: {matched / RECOMMEND: reduce frequency}
  Maintenance: {VACUUM scheduled / RECOMMEND: add VACUUM job}

COST SUMMARY
------------
  Estimated Databricks cost (current): ${Y}/month
  Estimated Databricks cost (with recommendations): ${Z}/month
  Potential savings: ${Y-Z}/month ({P}%)

PRIORITY ACTIONS
----------------
1. {Highest impact action — usually incrementalisation of largest table}
2. {Second highest}
3. {Third}
```

### Priority Ranking

Rank recommendations by estimated cost impact:

| Action | Typical Savings | Applies To |
|---|---|---|
| Incrementalise large tables (> 10GB) | 80-90% compute reduction per table | Both |
| Switch to serverless | Eliminates idle cluster costs + startup overhead | Both |
| Convert all-purpose clusters to job clusters | 40-60% — stops paying for idle time | PySpark |
| Enable spot instances on workers | 60-90% on worker node costs | PySpark |
| Enable Photon | 2-8x throughput at same cost | Both |
| Add liquid clustering | 50-90% scan reduction for filtered queries | Both |
| Fix missing table properties | 10-30% storage + maintenance improvement | Both |
| Add job timeouts | Prevents runaway cost from stuck jobs | PySpark |
| Match schedule to data frequency | Eliminates wasted runs | Both |
| Add VACUUM schedule | Ongoing storage cost reduction | Both |

---

## Tips

- The biggest wins are almost always incrementalisation and serverless — start there
- For PySpark jobs, the #1 quick win is often switching from all-purpose to job clusters (or serverless) and enabling spot instances
- For SDP pipelines, the #1 quick win is converting materialized views to streaming tables on large tables
- Don't recommend liquid clustering for tables < 10GB — the overhead isn't worth it
- CDF enablement on source tables is a prerequisite for both SDP streaming tables and PySpark readStream — flag it early
- When recommending edition downgrade (ADVANCED → CORE), check that no ADVANCED-only features are in use first
- PySpark jobs using `write.mode("overwrite")` on tables > 10GB are the biggest cost anti-pattern — always flag these
- Check Workflow schedules against actual data update frequency — over-scheduling is silent cost waste

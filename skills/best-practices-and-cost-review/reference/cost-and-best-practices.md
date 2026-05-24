# Cost Optimisation & Best Practices Reference

Detailed checklist for auditing Databricks SDP (Spark Declarative Pipelines) for cost efficiency, storage best practices, and incremental processing patterns.

---

## 1. Incrementalisation Patterns

**This is the single biggest cost lever.** A table that full-recomputes 50GB daily when it could process a 500MB delta is burning ~100x more compute than necessary.

### SDP Table Types and When to Use Each

| SDP Type | How It Works | Best For | Cost Profile |
|---|---|---|---|
| **Streaming Table** | Processes only new/changed data via Spark Structured Streaming. SDP manages checkpoints automatically. | Append-only data, CDC-enabled sources, event logs | Lowest — scales with data change rate, not total size |
| **Materialized View** | Full recompute on every pipeline refresh. Output is a complete, idempotent snapshot. | Small dimension/reference tables (< 1GB), complex aggregations where incremental is impossible | Highest for large tables — cost scales with total table size |
| **Materialized View with incremental hints** | Uses `spark_conf` settings to enable skip/prune optimisation. Still technically a full refresh but the engine can short-circuit unchanged partitions. | Medium tables where streaming isn't possible but full scan is wasteful | Medium — depends on how well the engine can prune |

### Enabling Change Data Feed (CDF) for Incremental Reads

CDF is the foundation for efficient incremental processing in Databricks. Enable it on source tables:

```sql
-- Enable CDF on a source table
ALTER TABLE {catalog}.{schema}.{table}
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Verify CDF is enabled
SHOW TBLPROPERTIES {catalog}.{schema}.{table} ('delta.enableChangeDataFeed');
```

Then in the SDP pipeline:

```python
# BEFORE: Full read (materialized view pattern)
@dlt.table(name="my_target")
def my_target():
    return spark.read.table("source_table")

# AFTER: Incremental read via streaming (streaming table pattern)
@dlt.table(name="my_target")
def my_target():
    return spark.readStream.table("source_table")
```

### Converting a Materialized View to a Streaming Table

Step-by-step:

1. **Enable CDF on the source table** (see above)
2. **Change the SDP table definition:**
   - Replace `spark.read.table()` with `spark.readStream.table()`
   - Or replace `dlt.read()` with `dlt.readStream()`
3. **Handle initial backfill:** First run of a streaming table will process all existing data (full backfill). Subsequent runs process only changes.
4. **Test:** Run the pipeline in dev and verify row counts match after the initial backfill
5. **Monitor:** Check pipeline run metrics — DBU consumption should drop dramatically after the first run

### When NOT to Incrementalise

- **Reference/dimension tables < 1GB** — full recompute is cheap and guarantees correctness
- **Tables with complex, non-monotonic update patterns** — e.g., late-arriving corrections that update random historical rows
- **One-time or ad-hoc history loads** — these run once and don't need incremental patterns
- **Tables where the source does not support CDF and has no reliable watermark** — can't do incremental without a change signal

---

## 2. Table Storage Best Practices

### Delta Table Properties Checklist

Run `SHOW TBLPROPERTIES {table}` and check:

| Property | Recommended Value | Impact |
|---|---|---|
| `delta.enableChangeDataFeed` | `true` | Required for downstream incremental reads. Without this, downstream consumers must full-scan. |
| `delta.autoOptimize.optimizeWrite` | `true` | Coalesces small output files on write. Prevents the small file problem. |
| `delta.autoOptimize.autoCompact` | `true` | Background compaction of accumulated small files. |
| `delta.enableDeletionVectors` | `true` | Faster deletes/updates — marks rows as deleted without rewriting entire files. |
| `delta.enableRowTracking` | `true` | Required for SDP incremental change tracking. |
| `delta.tuneFileSizesForRewrites` | `true` | For merge-heavy tables: optimises file sizes for frequent UPDATE operations. |
| `delta.targetFileSize` | `128mb` to `1gb` (depending on table size) | Controls target file size after OPTIMIZE. Larger tables benefit from larger files. |

### Liquid Clustering

Liquid clustering replaces traditional Hive-style partitioning and Z-ordering with a single, adaptive mechanism.

**When to apply:**
- Tables > 100GB
- Tables with 1-4 columns that appear in most WHERE clauses
- Tables currently using Hive-style partitioning

**How to apply:**

```sql
-- Add liquid clustering to an existing table
ALTER TABLE {catalog}.{schema}.{table}
CLUSTER BY ({col1}, {col2});

-- Check current clustering
DESCRIBE DETAIL {catalog}.{schema}.{table};
```

**How to pick clustering columns:**
1. Look at the WHERE clauses in the SDP pipeline code
2. Look at downstream query patterns (dashboards, BI tools)
3. Add high-cardinality filter columns (e.g., `customer_id`, `region`)
4. Max 4 columns — beyond that, diminishing returns

**Converting Hive-style partitions to liquid clustering:**

If a table still uses Hive-style partitioning (e.g., `PARTITIONED BY (partition_date)`):
- Convert to liquid clustering: `ALTER TABLE ... CLUSTER BY (partition_date)` and drop the partition spec
- This is almost always a net improvement: no partition pruning limits, adaptive layout, better for mixed workloads

### Small File Problem

Check for small files:

```sql
DESCRIBE DETAIL {catalog}.{schema}.{table}
```

**Red flags:**
- `numFiles` > 10,000 with `sizeInBytes` < 10GB → too many small files
- Average file size < 32MB → significant read overhead

**Fix:**
```sql
-- Compact small files
OPTIMIZE {catalog}.{schema}.{table};

-- For tables with liquid clustering
OPTIMIZE {catalog}.{schema}.{table};  -- clustering is applied automatically
```

If `autoOptimize` is enabled (it should be), this happens automatically over time.

---

## 3. Compute Configuration

### Serverless vs Classic SDP

| Aspect | Serverless SDP | Classic SDP (with clusters) |
|---|---|---|
| **Cost model** | Pay per DBU consumed | Pay for cluster uptime (even idle) |
| **Scaling** | Automatic, instant | Autoscaling with lag |
| **Management** | Zero — no cluster config | Must size and configure clusters |
| **Recommendation** | **Default choice for all new pipelines** | Only if specific Spark configs or init scripts are required |

### Pipeline Edition Selection

| Edition | Features | When to Use |
|---|---|---|
| `CORE` | Basic SDP — materialized views, streaming tables | Default for most pipelines |
| `PRO` | CORE + enhanced CDC, expectations with quarantine | When you need row-level CDC propagation or data quality quarantine |
| `ADVANCED` | PRO + enhanced autoscaling, enhanced monitoring | Large, complex pipelines with many tables and variable load |

**Cost implication:** ADVANCED costs ~2x CORE per DBU. Don't use ADVANCED unless you have a specific reason.

### Photon

Photon should be enabled for SQL-heavy transformations. It provides 2-8x speedup on common patterns (joins, aggregations, filters) at the same DBU cost.

Check the pipeline config:
```yaml
# In resources/*_dlt_bundle.yml
pipelines:
  my_pipeline:
    photon: true  # Should be true
```

---

## 4. Maintenance & Housekeeping

### VACUUM

Removes old file versions that are no longer referenced by the current table version. Reduces storage costs.

```sql
-- Check current retention setting
SHOW TBLPROPERTIES {catalog}.{schema}.{table} ('delta.deletedFileRetentionDuration');

-- Run vacuum (default 7-day retention)
VACUUM {catalog}.{schema}.{table};

-- For tables with high write frequency, consider shorter retention
ALTER TABLE {catalog}.{schema}.{table}
SET TBLPROPERTIES (delta.deletedFileRetentionDuration = 'interval 2 days');
```

**Warning:** Don't set retention below 7 days unless you're sure no long-running queries depend on historical file versions.

### ANALYZE TABLE

Updates column-level statistics used by the query optimizer. Run after major data loads:

```sql
ANALYZE TABLE {catalog}.{schema}.{table} COMPUTE STATISTICS FOR ALL COLUMNS;
```

This improves join order selection, predicate pushdown, and broadcast join decisions.

---

## 5. Cost Estimation Queries

### Estimate SDP Pipeline DBU Consumption

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

### Estimate PySpark Job DBU Consumption

```sql
-- Job run history
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

### DBU Consumption by Job or Pipeline (Both Types)

```sql
SELECT
  sku_name,
  usage_date,
  SUM(usage_quantity) AS total_dbus,
  usage_metadata.job_id,
  usage_metadata.pipeline_id
FROM system.billing.usage
WHERE usage_metadata.job_id = '{job_id}'
  OR usage_metadata.pipeline_id = '{pipeline_id}'
GROUP BY 1, 2, 4, 5
ORDER BY usage_date DESC
LIMIT 30
```

### Storage Cost Drivers

```sql
-- Find tables with excessive file counts or versions
SELECT
  table_name,
  num_files,
  size_in_bytes / (1024*1024*1024) AS size_gb,
  CASE WHEN num_files > 0 THEN size_in_bytes / num_files / (1024*1024) ELSE 0 END AS avg_file_size_mb
FROM (
  SELECT
    '{table}' AS table_name,
    numFiles AS num_files,
    sizeInBytes AS size_in_bytes
  FROM (DESCRIBE DETAIL {catalog}.{schema}.{table})
)
```

---

## 6. Anti-Patterns to Flag

### SDP-Specific

| Anti-Pattern | Why It's Bad | Fix |
|---|---|---|
| Full recompute on tables > 10GB | Burns compute on unchanged data every run | Convert to streaming table with CDF |
| `ADVANCED` edition on simple pipelines | 2x cost for features you don't use | Downgrade to `CORE` or `PRO` |
| `spark.table()` without pushdown filters in SDP views | Reads entire source table on every refresh | Add `.filter()` or use `spark.readStream` |

### PySpark Job-Specific

| Anti-Pattern | Why It's Bad | Fix |
|---|---|---|
| `write.mode("overwrite").saveAsTable()` on tables > 10GB | Rewrites all data even if 1% changed | Convert to MERGE INTO or partition-level overwrite |
| Running on all-purpose clusters | Paying for idle time between and after runs | Switch to job clusters or serverless |
| No spot instances on workers | Paying full on-demand price | Enable spot for worker nodes, keep driver on-demand |
| No job timeout configured | A stuck job runs (and charges) until manually killed | Set max timeout in job config |
| Hourly schedule on daily-updating data | 23 wasted runs per day | Match schedule to source update frequency |
| Large cluster for small data (< 10GB) | Over-provisioned, most cores idle | Downsize or switch to serverless |
| `spark.read.table()` without filter on large source | Full table scan every run | Add watermark filter: `.filter(F.col("updated_at") > last_run)` |

### Shared (Both Workload Types)

| Anti-Pattern | Why It's Bad | Fix |
|---|---|---|
| Manual HWM tracking (external store/Delta) | Fragile, error-prone, redundant with native Databricks features | SDP: streaming tables. PySpark: MERGE with watermark or readStream. |
| No `autoOptimize` on write-heavy tables | Accumulates small files → slow reads, high storage | Enable `delta.autoOptimize.optimizeWrite` and `delta.autoOptimize.autoCompact` |
| Hive-style partitioning with high cardinality | Too many partitions → metadata overhead, small files | Convert to liquid clustering |
| No VACUUM schedule | Unbounded storage growth from old file versions | Add VACUUM to maintenance job or rely on predictive optimisation |
| CDF not enabled on source tables | Downstream consumers must full-scan | `ALTER TABLE SET TBLPROPERTIES (delta.enableChangeDataFeed = true)` |
| No Photon on SQL/DataFrame-heavy workloads | Missing 2-8x free speedup | Enable Photon in pipeline/cluster config |

---

## 7. Databricks Cost Components

| Cost Component | Databricks (SDP or PySpark) |
|---|---|
| **Compute** | DBU-hours (varies by SKU, typically $0.07-0.40/DBU) |
| **Compute model** | Serverless: pay per DBU. Classic: pay per cluster-hour. |
| **Storage** | S3/ADLS + Unity Catalog (included) |
| **Orchestration** | SDP native scheduling or Databricks Workflows |
| **Monitoring** | System tables + event logs (included) |
| **Data quality** | SDP Expectations (built-in) or custom assertions for PySpark |

**Key cost metrics to gather:**
1. Databricks DBU-hours per run × runs per day × DBU rate = daily compute cost
2. For PySpark jobs: include cluster startup time in cost calculation (or note elimination with serverless)
3. Factor in: built-in monitoring, native HWM via streaming tables, spot instance savings

# AWS Glue / Redshift → Databricks Conversion Reference

Source-specific reference for reviewing Databricks pipelines migrated from AWS Glue and/or Redshift. In the `dataworx_cc` repo Glue jobs typically write to Redshift, so the source code and Redshift DDLs live side-by-side and this file covers both. Load this file in Step 1 if the source system is `glue`, `redshift`, or both.

---

## 1. Finding Glue Job Code

Glue jobs follow this pattern in the `dataworx_cc` repo:

```
{domain}/{domain}/conform_zone/{pipeline-name}/custom/
├── glue_template.py              # Base class (shared)
├── glue-{name}.py                # Main ETL script
├── {entity}.py                   # Entity-specific logic classes
└── ddls/                         # Redshift DDL definitions (see §2)
    └── V{NNN}_{NN}_{table}.sql   # Flyway-versioned DDLs
```

Alternate locations:

```
{domain}/{domain}/use_cases/{pipeline-name}/custom/
{domain}/{domain}/data_prep/{pipeline-name}/custom/
{domain}/{domain}/data_ingestion/{source}/custom/
{domain}/_shared_resources/pipLibrary/
```

### Sparse-checkout pattern for the source side

When setting up a Databricks Repo for the Glue/Redshift source side:

```
{domain}/{domain}/
{domain}/_shared_resources/
```

This pulls the Glue jobs (`{domain}/{domain}/conform_zone/*/custom/`), Redshift DDLs, use cases, and shared libraries.

### Key Glue code patterns to grep for

```bash
# Find the Glue job for a specific table
grep -rl "table_name_here" {domain}/{domain}/

# Find all Glue entry points
find {domain}/{domain} -name "glue-*.py"

# Find SQL transformations embedded in Glue jobs
grep -n "spark.sql\|resolveChoice\|DynamicFrame" {domain}/{domain}/**/custom/*.py
```

### Glue job anatomy

```python
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions

class MyJob(GlueTemplate):
    def execute(self):
        # 1. Read from Glue Catalog (S3-backed)
        source = glueContext.create_dynamic_frame.from_catalog(
            database="raw_db", table_name="source_table"
        )
        # 2. Transform
        result = source.resolveChoice(choice="cast:string")
        df = result.toDF()
        df = df.filter(...)  # Business logic here
        # 3. Write to S3/Redshift
        df.write.mode("overwrite").parquet(s3_path)
        # 4. Update HWM in DynamoDB
        update_hwm(pipeline_name, new_hwm_value)
```

---

## 2. Finding Redshift DDLs

Redshift table definitions (used by Glue jobs as targets, or by direct Redshift ETL) live alongside the Glue source code:

```
{domain}/{domain}/conform_zone/{pipeline-name}/custom/ddls/
└── V{NNN}_{NN}_{table_name}.sql
```

DDL files contain:
- `CREATE TABLE` with Redshift types (`VARCHAR(N)`, `DECIMAL(P,S)`, `TIMESTAMP`)
- `PRIMARY KEY` definitions
- `DISTKEY` / `SORTKEY` declarations
- Audit columns (`dh_audit_created_datetime`, etc.)

### Useful for

- Understanding the original target schema and types
- Identifying `VARCHAR(N)` length constraints that Databricks won't enforce
- Finding primary/business keys for join comparisons
- Mapping Redshift `DISTKEY`/`SORTKEY` to Databricks liquid clustering

---

## 3. Cross-Reference: Source Table → Databricks Table

To find corresponding code across the migration boundary:

1. **Start with the Databricks table name** (e.g., `l3_mip_gd_contracts`)
2. **Search the SDP pipeline** for the table name:
   ```bash
   grep -rn "l3_mip_gd_contracts" {domain}/databricks_pipeline/
   ```
3. **Find the source table** referenced in the SDP code (usually a `spark.read.table()` or `spark.readStream.table()` call)
4. **Search the Glue codebase** for that source table or the target table:
   ```bash
   grep -rn "mip_gd_contracts\|gd_contracts" {domain}/{domain}/
   ```
5. **Compare the transformation logic** between the Glue `.py` file and the SDP `_dlt_pipeline.py`
6. **Compare the schema** against the Redshift DDL in `ddls/V{NNN}_{NN}_{table}.sql`

---

## 4. Glue → Databricks Conversion Fidelity

| Check | What to Compare |
|---|---|
| **Business logic** | SQL queries, joins, filters, transformations — is the logic preserved? |
| **Column completeness** | Are all source columns carried through, or were some dropped? |
| **Write semantics** | Did append stay append, overwrite stay overwrite? Or did semantics change? |
| **Schema handling** | Glue `resolveChoice()` vs Databricks explicit typing — any differences? |
| **Spark configs** | `datetime.rebase.mode`, broadcast thresholds, shuffle partitions — preserved? |
| **Type casting** | Explicit casts in Glue vs Databricks — any lossy conversions? |
| **Table naming** | Did target table names change? Are they mapped correctly? |
| **Partition keys** | Same partition columns? Or changed during migration? |
| **Date arithmetic** | Glue SQL interval expressions vs Databricks Python — see "Common gotchas" below |
| **HWM/incremental logic** | DynamoDB HWM in Glue vs SDP streaming or MERGE in Databricks |

### Detailed checklist

#### Business Logic
- [ ] All SQL `WHERE` clauses preserved with equivalent logic
- [ ] `JOIN` types match (INNER, LEFT, FULL OUTER)
- [ ] Aggregation `GROUP BY` columns match
- [ ] `CASE WHEN` / `IF-ELSE` logic preserved

#### Date Arithmetic (common gotcha)
- [ ] Glue SQL `date_trunc() + interval` expressions translated correctly
- [ ] Remember: `date_trunc('year', current_date) - interval '1 day' + interval '2 years'` evaluates differently than `f"{current_year + 2}-12-31"` — work through the arithmetic step by step
- [ ] `DATEDIFF` / `DATEADD` semantics match between Glue (PostgreSQL-style) and Spark SQL

#### Type Handling
- [ ] Glue `resolveChoice()` calls have equivalent explicit typing in Databricks
- [ ] `VARCHAR(N)` truncation from Redshift is documented as a known acceptable difference (Databricks does not enforce length)
- [ ] `DECIMAL` precision matches or differences are documented

#### Write Semantics
- [ ] Append stayed append, overwrite stayed overwrite
- [ ] Partition overwrite mode matches (`dynamic` vs `static`)
- [ ] If semantics changed (e.g., overwrite → streaming append), it's intentional and documented

#### Spark Config Preservation
- [ ] `spark.sql.legacy.timeParserPolicy` preserved if set in Glue
- [ ] `spark.sql.legacy.parquet.datetimeRebaseModeInRead` preserved
- [ ] `spark.sql.adaptive.enabled` settings carried over
- [ ] Broadcast join thresholds preserved if explicitly set

#### HWM / Incremental Logic
- [ ] DynamoDB HWM tracking in Glue replaced with SDP streaming or MERGE watermark
- [ ] The transition point (where incremental processing begins) is correct
- [ ] No data gaps between the last Glue run and the first Databricks run

---

## 5. Redshift → Databricks Conversion Fidelity

| Check | What to Compare |
|---|---|
| **Type mapping** | Redshift types → Databricks types — any precision loss? |
| **VARCHAR enforcement** | Redshift enforces length truncation; Databricks does not — verify downstream consumers don't depend on truncation |
| **DISTKEY → clustering** | Redshift `DISTKEY` columns → Databricks liquid clustering columns (good starting point but not always optimal) |
| **SORTKEY → clustering** | Redshift `SORTKEY` columns → Databricks liquid clustering or Z-ordering |
| **Compound SORTKEY** | Multi-column `SORTKEY` → multi-column `CLUSTER BY` (max 4 columns recommended in Databricks) |
| **Identity columns** | Redshift `IDENTITY(start, step)` → Databricks `GENERATED ALWAYS AS IDENTITY` |
| **SQL dialect** | Redshift (PostgreSQL-style) SQL → Spark SQL — any function differences? |
| **Date arithmetic** | Redshift `DATEADD`/`DATEDIFF`/interval syntax → Spark SQL equivalents |
| **Window functions** | Redshift OLAP functions → Spark window function equivalents |

### Type Mapping Reference

| Redshift Type | Databricks Type | Notes |
|---|---|---|
| `VARCHAR(N)` | `STRING` | Length not enforced — flag if downstream depends on truncation |
| `CHAR(N)` | `STRING` | Padding not preserved |
| `BOOLEAN` | `BOOLEAN` | Direct match |
| `SMALLINT` | `SMALLINT` | Direct match |
| `INTEGER` | `INT` | Direct match |
| `BIGINT` | `BIGINT` | Direct match |
| `DECIMAL(P,S)` / `NUMERIC(P,S)` | `DECIMAL(P,S)` | Verify precision/scale preserved |
| `REAL` | `FLOAT` | Direct match |
| `DOUBLE PRECISION` | `DOUBLE` | Direct match |
| `DATE` | `DATE` | Direct match |
| `TIMESTAMP` | `TIMESTAMP` | Verify timezone handling |
| `TIMESTAMPTZ` | `TIMESTAMP` | Spark TIMESTAMP is timezone-aware in UTC; verify |
| `SUPER` | `VARIANT` or `STRING` | Semi-structured — Databricks `VARIANT` is the closest |

### SQL Function Differences (common ones)

| Redshift | Spark SQL | Notes |
|---|---|---|
| `DATEADD(day, 1, dt)` | `date_add(dt, 1)` or `dt + INTERVAL 1 DAY` | |
| `DATEDIFF(day, a, b)` | `datediff(b, a)` | Argument order differs |
| `GETDATE()` | `current_timestamp()` | |
| `LISTAGG(col, ',')` | `concat_ws(',', collect_list(col))` | |
| `NVL(a, b)` | `coalesce(a, b)` or `nvl(a, b)` | Both work |
| `STRPOS(s, sub)` | `instr(s, sub)` | |
| `TRUNC(dt)` | `date_trunc('day', dt)` | |
| `REGEXP_SUBSTR` | `regexp_extract` | Slightly different signature |

---

## 6. Glue/Redshift-Era Anti-Patterns to Flag

| Anti-Pattern | Why It's Bad | Fix |
|---|---|---|
| **DynamoDB HWM tracking carried over** | Fragile, error-prone, redundant with native Databricks streaming/MERGE features | SDP: streaming tables. PySpark: MERGE with watermark or `readStream`. |
| **`DynamicFrame.resolveChoice()` patterns left in** | Glue-specific abstraction; Spark DataFrames handle typing directly | Replace with explicit `cast()` / `withColumn(F.col(x).cast(...))` |
| **`from_catalog()` reads** | Glue Data Catalog dependency; Databricks reads from Unity Catalog or direct paths | `spark.read.table("catalog.schema.table")` |
| **Bookmark-style state** | Glue job bookmarks for incremental tracking | Delta CDF / streaming checkpoints |
| **S3 write paths hardcoded** | Site-specific paths leak into code | Use bundle variables / Unity Catalog volumes |
| **VARCHAR truncation silently lost** | Redshift enforced length; Databricks doesn't — downstream may break on long values | Either add explicit truncate in the pipeline or document the change |
| **DISTKEY/SORTKEY blindly copied** | These were Redshift execution choices; liquid clustering picks may differ | Re-evaluate clustering columns based on actual Databricks query patterns |

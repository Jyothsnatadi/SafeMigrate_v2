# Pipeline Code Review Checklist

Comprehensive source-agnostic checklist for reviewing Databricks pipelines. Each item includes what to look for and common violations. For source-specific conversion-fidelity checks (Glue, Redshift, Informatica, etc.), see `sources/{source}.md`.

---

## 1. Databricks Best Practices

### SDP (Spark Declarative Pipelines) Usage

#### `import dlt` → `pyspark.pipelines` (HIGH severity)

- [ ] `import dlt` must be replaced with `from pyspark import pipelines as dp`
- [ ] `@dlt.table` must be replaced with `@dp.materialized_view` (for batch/snapshot reads) or `@dp.table` (for streaming reads)
- [ ] `@dlt.view` must be replaced with `@dp.view`
- [ ] `dlt.read()` must be replaced with `spark.read.table()`
- [ ] `dlt.read_stream()` must be replaced with `spark.readStream.table()`

#### General SDP Checks

- [ ] Is SDP used where appropriate? (declarative table definitions, expectations, streaming)
- [ ] Are `@dp.materialized_view`, `@dp.table`, and `@dp.view` decorators used correctly for their read mode?
- [ ] Are table properties set (`delta.enableChangeDataFeed`, `delta.enableRowTracking`, etc.)?
- [ ] Are SDP Expectations defined for data quality checks?

**Common violations:**
```python
# BAD: Legacy dlt module
import dlt

@dlt.table(name="my_table")
def my_table():
    return dlt.read("source_view")

# GOOD: pyspark.pipelines with correct decorator for batch read
from pyspark import pipelines as dp

@dp.materialized_view(name="my_table")
def my_table():
    return spark.read.table("catalog.schema.source")

# GOOD: pyspark.pipelines with correct decorator for streaming read
@dp.table(name="my_streaming_table")
def my_streaming_table():
    return spark.readStream.table("catalog.schema.source")
```

### Incremental Processing

- [ ] Are large tables (> 1GB) using incremental patterns?
- [ ] SDP: Are streaming tables used for append/CDC sources?
- [ ] PySpark: Is MERGE INTO or partition overwrite used instead of full overwrite?
- [ ] Is CDF enabled on source tables for downstream incremental reads?
- [ ] Are history load notebooks designed as one-time runs, not recurring full scans?

**Common violations:**
```python
# BAD: Full overwrite on large table
df.write.mode("overwrite").saveAsTable("target")

# GOOD: MERGE for upserts
target.alias("t").merge(source.alias("s"), "t.key = s.key") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

### Auto Loader

- [ ] Is Auto Loader (`cloudFiles`) used for file-based ingestion from S3/ADLS?
- [ ] Is `cloudFiles.format` set correctly?
- [ ] Is schema evolution configured (`cloudFiles.schemaEvolutionMode`)?
- [ ] Is `_rescued_data` column handled or excluded?

### Schema Handling

- [ ] Is `mergeSchema` enabled where schema evolution is expected?
- [ ] Are explicit schema definitions used where schema should be fixed?
- [ ] Is `_rescued_data` handled appropriately?
- [ ] Are nullable/non-nullable constraints correct?

---

## 2. Cost Efficiency

### Column Pruning

- [ ] No `SELECT *` in SQL queries — select only needed columns
- [ ] No `spark.table("x")` without subsequent `.select()` — reads all columns
- [ ] DataFrame operations select columns early in the chain
- [ ] Unused source ports/columns are not carried through needlessly

**Common violations:**
```python
# BAD: Reads all columns, filters late
df = spark.table("big_table")
result = df.filter(df.status == "active").select("id", "name")

# GOOD: Select early
df = spark.table("big_table").select("id", "name", "status")
result = df.filter(df.status == "active")
```

### No `display()` / `.show()` in Production Code (HIGH severity)

- [ ] No `display()` calls in production pipeline code — triggers full evaluation, causes job failures in non-interactive contexts
- [ ] No `.show()` calls in production pipeline code — same issue

**Common violations:**
```python
# BAD: display() in production pipeline — will fail in job/SDP context
df = spark.table("catalog.schema.my_table")
display(df)  # interactive-only — crashes in scheduled runs

# BAD: .show() left in from debugging
df.show(100)

# GOOD: Remove entirely, or guard behind a debug flag
if spark.conf.get("pipeline.debug_mode", "false") == "true":
    df.show(5)
```

### Unnecessary Scans

- [ ] No `.count()` calls just for logging — triggers full scan
- [ ] No `collect()` on large DataFrames
- [ ] Lookup-style joins don't re-read the same dimension table on every call — broadcast or cache once

**Common violations:**
```python
# BAD: Full scan just to log a count
logger.info(f"Source has {source_df.count()} rows")

# GOOD: Skip the count or use approximate
# (or log count only in debug mode)
```

### Window Functions

- [ ] All `ROW_NUMBER()` / `RANK()` / `DENSE_RANK()` have `PARTITION BY` clauses
- [ ] Window partitions are not too fine-grained (avoid partitioning by unique key)
- [ ] Window partitions are not missing entirely (single-partition shuffle)

**Common violations:**
```sql
-- BAD: No PARTITION BY — shuffles everything to one partition
ROW_NUMBER() OVER (ORDER BY updated_at DESC)

-- GOOD: Partitioned
ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC)
```

### Repartition

- [ ] `.repartition()` is not called without good reason
- [ ] If repartitioning, the target partition count is sensible (not 1, not 10000)
- [ ] `.coalesce()` is used instead of `.repartition()` when reducing partitions

---

## 3. DAB (Databricks Asset Bundle) Configuration

### Bundle Variables

- [ ] `${bundle.target}` is used (NOT `${bundle.environment}` — that's invalid)
- [ ] All variables referenced in pipeline code are defined in `databricks.yml`
- [ ] Variables are environment-specific where needed (dev/nonprod/prod catalogs, schemas)
- [ ] Source-system parameters (env vars, parameter files, `$$Param`, etc.) have been mapped to bundle variables or job parameters, not hardcoded — see source reference for source-specific mapping rules

**Common violations:**
```yaml
# BAD: Invalid variable
pipelines:
  name: ${bundle.environment}_my_pipeline  # environment is not a valid bundle var

# GOOD:
pipelines:
  name: ${bundle.target}_my_pipeline
```

### run_as

- [ ] Production jobs use service principal identity, not personal accounts
- [ ] `run_as` is configured at the job or pipeline level

### Compute

- [ ] If `serverless: true`, no cluster config is needed (absence is correct)
- [ ] If serverless compute has been specified, has the `performance_target` been set? If not then we should question whether the pipeline needs to run in the default PERFORMANCE_OPTIMIZED mode. Otherwise `performance_target` should be explicitly set with a preference for STANDARD.
- [ ] If not serverless, cluster config is appropriate (instance type, workers, autoscaling)
- [ ] Job clusters are used for jobs (not all-purpose clusters)
- [ ] Photon is enabled for SQL/DataFrame-heavy workloads

### Timeouts and Retries

- [ ] Jobs have a `timeout_seconds` configured
- [ ] Retry policy is set (max 1-2 retries, not unlimited)
- [ ] SDP pipelines have appropriate `development` mode settings per target

### Environment Configs

- [ ] Dev, nonprod, and prod targets exist with appropriate differences
- [ ] Catalog names differ per environment
- [ ] Permissions/grants are configured per environment
- [ ] Service principals differ per environment where required

### Connections & Secrets

- [ ] External source/target connections migrated to Databricks connections, secret scopes, or volume paths
- [ ] No plaintext usernames/passwords in code or YAML
- [ ] Connection strings reference secrets, not literal values

---

## 4. Code Quality

### Filter Logic

- [ ] AND/OR operators are correct in complex filters
- [ ] No tautologies (`x > 0 OR x <= 0` — always true)
- [ ] No contradictions (`x > 5 AND x < 3` — always false)
- [ ] Null handling in filters is correct (`IS NULL` vs `== None`)
- [ ] Default/fallthrough groups in routing/filtering logic are handled — rows that don't match any output must go somewhere explicit (drop, quarantine, or log)

### Hardcoded Values

- [ ] No hardcoded catalog names — should use bundle variables
- [ ] No hardcoded schema names — should use bundle variables
- [ ] No hardcoded dates (except in one-time history loads)
- [ ] No hardcoded file paths — should use variables or Auto Loader
- [ ] No hardcoded connection strings or credentials (should be in secrets)

**Common violations:**
```python
# BAD: Hardcoded catalog
df = spark.table("avi_insighthub_d_curated_reg.conform_main_aviation.my_table")

# GOOD: Parameterised
catalog = spark.conf.get("pipeline.target_catalog")
df = spark.table(f"{catalog}.{schema}.my_table")
```

### Code Duplication

- [ ] No copy-pasted transformation logic across sub-pipelines
- [ ] Shared logic is extracted to utility functions or shared modules
- [ ] Source-system reusable units (mapplets, shared Glue libraries) have been turned into shared Python modules or shared SDP views — not pasted into every pipeline

### Dead Code

- [ ] No unused imports
- [ ] No commented-out code blocks (delete them, git has the history)
- [ ] No unreachable code paths (after unconditional return/raise)
- [ ] No unused variables
- [ ] No ports/columns from the source mapping carried through unused

### Naming

- [ ] Table names follow a consistent convention (e.g., `l3_`, `l4_` layer prefixes)
- [ ] Column names are consistent (snake_case, no mixed conventions)
- [ ] Function names describe what they do
- [ ] No single-letter variable names in production code (except loop counters)

---

## 5. Testing

### Unit Tests

- [ ] Unit tests exist for transformation logic
- [ ] Tests are not placeholders (`assert True`, `pass`, empty bodies)
- [ ] Tests use realistic test data, not trivially empty DataFrames
- [ ] Tests assert specific expected values, not just "no exception thrown"
- [ ] Edge cases are tested (nulls, empty strings, boundary dates)

**Common violations:**
```python
# BAD: Placeholder test
def test_pipeline():
    assert True

# BAD: Only tests "no crash"
def test_transform():
    result = transform(test_df)
    assert result is not None

# GOOD: Tests actual logic
def test_transform():
    result = transform(test_df)
    assert result.count() == 5
    assert result.filter(F.col("status") == "active").count() == 3
```

### Validation / Reconciliation Scripts

- [ ] QA notebooks check actual data values, not just print/display
- [ ] Validation failures raise exceptions or set exit codes (not just print warnings)
- [ ] Validation results are persisted or reported, not just displayed
- [ ] Reconciliation against the original source output exists — at minimum row counts; ideally per-column hashes or aggregates for a sample window

### Exception Handling

- [ ] No bare `except: pass` in production code
- [ ] Exceptions are logged with context before being re-raised or handled
- [ ] Validation exceptions fail the pipeline (not silently swallowed)

---

## 6. Curation Framework Standards (commonlibraries)

### Installation & Imports

- [ ] `commonlibraries` is installed using `dbutils.secrets.get(scope="databricks-package-management", key="pip-index-url")` for the index URL
- [ ] Imports are from the module file directly, not the package level

**Common violations:**
```python
# BAD: Package-level import — will not resolve
from commonlibraries.reader import DeltaTableReader

# GOOD: Direct module import
from reader.reader import DeltaTableReader, StreamingDeltaReader
from transformation.transformation import BaseTransformation
from quality_rules.quality_rules import BaseQualityRule
```

### Reader Usage (LOW severity)

- [ ] `DeltaTableReader` is used for batch Delta reads
- [ ] `StreamingDeltaReader` is used for streaming Delta reads
- [ ] `validate_configuration()` is called before `read_data()`
- [ ] Custom readers extend `BaseDLTReader` and implement `read_data()`
- [ ] Custom readers accept `spark` and `table_name` via `super().__init__()`

### Delta Table Properties (HIGH severity)

- [ ] Default table properties are understood (`delta.enableChangeDataFeed=true`, `delta.enableRowTracking=true`, `delta.enableDeletionVectors=true`, `delta.deletedFileRetentionDuration=interval 30 days`)
- [ ] If overriding defaults, the override is intentional and documented
- [ ] CDF is not inadvertently disabled when downstream consumers depend on it

### Transformation Usage

- [ ] Built-in transformation classes are used where applicable instead of writing raw PySpark equivalents
- [ ] Transformations to check for: `ColumnRenameTransformation`, `NullHandlingTransformation`, `DeduplicationTransformation`, `FilterTransformation`, `DataTypeCastingTransformation`, `DateParsingTransformation`, `WindowFunctionTransformation`
- [ ] Custom transformations extend `BaseTransformation` and implement `transform(df)`
- [ ] Custom transformations call `self.validate_input(df, required_columns=[...])` at the start
- [ ] Custom transformations call `self.validate_output(df, expected_columns=[...])` at the end
- [ ] Custom transformations do NOT call `pre_transform()`/`post_transform()` manually — the base class manages these

**Common violations:**
```python
# BAD: Raw PySpark when a built-in transformation exists
df = df.dropDuplicates(["vehicle_id", "event_ts"])

# GOOD: Use the library class
from transformation.transformation import DeduplicationTransformation
df = DeduplicationTransformation(subset=["vehicle_id", "event_ts"], keep="first").transform(df)
```

### Quality Rules Usage

- [ ] Data quality checks use the `quality_rules` module or SDP expectations
- [ ] Built-in rules are used where applicable: `NullCheckRule`, `UniqueValueRule`, `AllowedValuesRule`, `DataTypeRule`, `RowCountThresholdRule`, `PatternMatchRule`
- [ ] Custom rules extend `BaseQualityRule` and implement `check(df)`
- [ ] `check(df)` returns a dict with at minimum `rule_id` and `status` (`"PASSED"`/`"FAILED"`)
- [ ] `self.log_result(result)` is called before returning
- [ ] `self.validate_input_schema(df, required_columns=[...])` is called at the top of `check()`

### Known Pitfalls

- [ ] `ChangeDetectionTransformation` is called with two DataFrame arguments (`transform(old_df, new_df)`), not one
- [ ] `ReferenceIntegrityRule` is only used with small reference tables (it calls `.collect()` on the reference DF)
- [ ] `from_dict()` on concrete subclasses is overridden if the subclass has extra constructor arguments beyond the base four
- [ ] `WindowFunctionTransformation` with `lag`/`lead` — verify `target_column` is correct (it's used as both source and output column name)

---

## 7. Source Conversion Fidelity

Source-specific conversion-fidelity checks live in their own reference files. Load the one matching the user's source system:

- `sources/aws-glue-redshift.md` — AWS Glue and Redshift (DDLs, types, SQL) to Databricks conversion fidelity
- `sources/informatica.md` — Informatica (PowerCenter or IICS/IDMC) to Databricks conversion fidelity
- `sources/teradata.md` — Teradata (BTEQ, TPT, stored procedures, DDL) to Databricks conversion fidelity
- `sources/oracle.md` — Oracle (PL/SQL, sequences, triggers, MVs) to Databricks conversion fidelity

---

## 8. Migration Hand-off (universal)

These aren't code-quality issues per se, but they belong in the review.

- [ ] **Source-target mapping document** exists — every Databricks table can be traced back to its source artefact(s)
- [ ] **Shared/reused logic** in the source system has been turned into shared modules/views in Databricks — verify by grepping for duplicated logic
- [ ] **Reconciliation runs** comparing source output vs Databricks output exist for at least a representative window (row counts at minimum, ideally column hashes)
- [ ] **Cut-over plan** addresses any state that the source system owned (HWM values, mapping variables, sequence values, last-run timestamps) — that state has been bootstrapped on the Databricks side
- [ ] **Schedules** (cron / event-based triggers) from the source system have equivalent Databricks Workflow triggers, with matching SLA expectations

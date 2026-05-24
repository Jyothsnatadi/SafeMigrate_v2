# Teradata → Databricks Conversion Reference

Source-specific reference for reviewing Databricks pipelines migrated from Teradata. Load this file in Step 1 if the source system is `teradata`.

---

## 1. Finding Teradata Source Artefacts

Teradata exports typically include:

```
{teradata_root}/
├── ddl/                    *.sql        # CREATE TABLE / VIEW / INDEX
├── views/                  *.sql        # CREATE VIEW definitions
├── bteq/                   *.btq        # Batch SQL scripts (BTEQ)
├── tpt/                    *.tpt        # Teradata Parallel Transporter load/unload scripts
├── stored_procedures/      *.sp         # Stored procedures
├── macros/                 *.sql        # Reusable macros
└── scripts/                *.sh         # Wrapper shell scripts invoking BTEQ/TPT
```

### What to locate for a given Databricks pipeline

- DDL for each target table (PRIMARY INDEX, PPI, etc.)
- BTEQ scripts that loaded the table
- Any stored procedures called by those scripts
- Macros referenced in the BTEQ
- TPT scripts if data was loaded from external files

### Key Teradata code patterns to grep for

```bash
# Find loaders for a specific table
grep -rln "INSERT INTO.*table_name\|UPDATE.*table_name\|MERGE INTO.*table_name" {teradata_root}/bteq/

# Find DDL for a table
find {teradata_root}/ddl -name "*table_name*.sql"

# Find QUALIFY clauses (Teradata-specific row-filter window)
grep -rn "QUALIFY" {teradata_root}/
```

### BTEQ script anatomy

```sql
.LOGON tdpid/user,password;
.SET ERROROUT STDOUT;

DATABASE my_db;

DELETE FROM my_db.target_table WHERE load_date = CURRENT_DATE;

INSERT INTO my_db.target_table
SELECT
  col1,
  col2,
  TRIM(col3) AS col3,
  CURRENT_DATE AS load_date
FROM my_db.source_table
WHERE etl_status = 'READY';

.QUIT;
```

---

## 2. Conversion Fidelity Checks

| Check | What to Compare |
|---|---|
| **DDL → Databricks DDL** | Column list, types, NULLs, defaults preserved |
| **PRIMARY INDEX → clustering** | Teradata PI (data distribution) re-evaluated as candidate clustering column |
| **PPI → partition/clustering** | Partitioned Primary Index → Databricks partitioning or liquid clustering |
| **SET vs MULTISET** | SET tables enforce uniqueness; MULTISET allow duplicates — verify dedup logic in Databricks matches |
| **QUALIFY** | `QUALIFY ROW_NUMBER() OVER (...) = 1` → Spark subquery with `WHERE row_number = 1` |
| **BTEQ control flow** | `.IF` / `.GOTO` / `.LABEL` → Workflow tasks with `run_if` conditions, or notebook control flow |
| **Stored procedures** | Procedural SQL → notebook task or rewritten DataFrame chain (no per-row cursors in Spark) |
| **Macros** | Reusable SQL macros → shared SQL functions, views, or Python utilities |
| **TPT loads** | Bulk load scripts → Auto Loader / `spark.read.format(...)` / `COPY INTO` |
| **Type mapping** | Teradata types → Databricks types — any precision loss? |
| **Date functions** | Teradata DATE/TIME/TIMESTAMP behaviour → Spark equivalents |
| **SAMPLE clause** | Teradata `SAMPLE N` → Spark `.sample(...)` or `.orderBy(rand()).limit(N)` |
| **Volatile/temp tables** | `VOLATILE` / `GLOBAL TEMPORARY` → temp views or Delta-backed scratch tables |
| **Identity columns** | Teradata `GENERATED ALWAYS AS IDENTITY` → Databricks `GENERATED ALWAYS AS IDENTITY` |

### Detailed checklist

#### DDL
- [ ] All columns from Teradata DDL present in Databricks table
- [ ] Type mapping correct (see §3)
- [ ] `NOT NULL` constraints preserved
- [ ] `DEFAULT` clauses preserved

#### Indexing → Clustering
- [ ] PRIMARY INDEX columns evaluated for liquid clustering (not always 1:1 — PI optimises for distribution, clustering optimises for query patterns)
- [ ] PPI (Partitioned PI) columns → Databricks partition columns OR liquid clustering
- [ ] Secondary indexes (USI, NUSI) — do NOT need direct equivalents; Databricks uses file pruning + clustering

#### QUALIFY
- [ ] `QUALIFY` clauses translated correctly:
  ```sql
  -- Teradata
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn
  FROM t
  QUALIFY rn = 1;

  -- Spark equivalent
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn FROM t
  ) WHERE rn = 1;
  ```

#### Stored Procedures
- [ ] Procedural logic decomposed into discrete pipeline steps, not translated as per-row cursors
- [ ] Dynamic SQL in procs reviewed for SQL injection risk
- [ ] Output parameters / status codes replaced with deterministic returns or quarantine tables

---

## 3. Type Mapping Reference

| Teradata Type | Databricks Type | Notes |
|---|---|---|
| `BYTEINT` | `TINYINT` | Direct match |
| `SMALLINT` | `SMALLINT` | Direct match |
| `INTEGER` / `INT` | `INT` | Direct match |
| `BIGINT` | `BIGINT` | Direct match |
| `DECIMAL(P,S)` / `NUMERIC(P,S)` | `DECIMAL(P,S)` | Verify precision/scale preserved |
| `FLOAT` / `REAL` / `DOUBLE PRECISION` | `DOUBLE` | Spark FLOAT is 32-bit; prefer DOUBLE |
| `NUMBER` (no precision) | `DECIMAL(38, ?)` | Verify default precision/scale assumption |
| `CHAR(N)` | `STRING` | Padding not preserved |
| `VARCHAR(N)` | `STRING` | Length not enforced — flag if downstream depends on truncation |
| `CLOB` | `STRING` | Large text |
| `BLOB` | `BINARY` | Large binary |
| `DATE` | `DATE` | Direct match |
| `TIME` | `STRING` or `TIMESTAMP` | Spark has no TIME type |
| `TIMESTAMP(N)` | `TIMESTAMP` | Spark precision is microseconds |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP` | Spark TIMESTAMP is timezone-aware in UTC |
| `INTERVAL YEAR TO MONTH` / `DAY TO SECOND` | `INTERVAL` or compute manually | |
| `PERIOD(DATE)` / `PERIOD(TIMESTAMP)` | Two columns: `start_*` and `end_*` | Spark has no PERIOD type |
| `BYTE(N)` / `VARBYTE(N)` | `BINARY` | |

---

## 4. SQL Function Differences

| Teradata | Spark SQL | Notes |
|---|---|---|
| `ADD_MONTHS(dt, n)` | `add_months(dt, n)` | Direct match |
| `MONTHS_BETWEEN(a, b)` | `months_between(a, b)` | Direct match |
| `EXTRACT(YEAR FROM dt)` | `year(dt)` or `extract(YEAR FROM dt)` | |
| `TRIM(BOTH 'x' FROM s)` | `trim(BOTH 'x' FROM s)` | Works in Spark |
| `CHARACTER_LENGTH(s)` / `CHAR_LENGTH(s)` | `length(s)` | |
| `POSITION(sub IN s)` | `instr(s, sub)` | |
| `SUBSTRING(s FROM p FOR n)` | `substring(s, p, n)` | |
| `CAST(x AS DATE FORMAT 'YYYY-MM-DD')` | `to_date(x, 'yyyy-MM-dd')` | Format token differences |
| `NULLIFZERO(x)` | `nullif(x, 0)` | |
| `ZEROIFNULL(x)` | `coalesce(x, 0)` | |
| `QUALIFY ROW_NUMBER() = 1` | Subquery + `WHERE row_number = 1` | No direct QUALIFY |
| `RECURSIVE WITH ...` | `WITH RECURSIVE` (DBSQL) or rewrite | |
| `SAMPLE N` | `LIMIT N` after `ORDER BY rand()` or `TABLESAMPLE` | Spark `LIMIT` alone is deterministic |
| `CAST(x AS DATE)` | `to_date(x)` or `cast(x AS DATE)` | |
| `DATEFORM=ANSIDATE` session setting | Not applicable — Spark uses ISO dates | |

---

## 5. Teradata-Era Anti-Patterns to Flag

| Anti-Pattern | Why It's Bad | Fix |
|---|---|---|
| **`PRIMARY INDEX` blindly copied to clustering** | PI optimises for distribution; clustering optimises for query patterns | Re-evaluate clustering columns based on Databricks query patterns |
| **PPI partition columns blindly copied** | Teradata partition counts/layouts ≠ optimal Spark partitioning | Choose partitioning based on data size and skew |
| **Per-row stored procedure logic translated as UDF/Python loop** | Defeats Spark parallelism | Decompose into DataFrame operations / SQL transforms |
| **`QUALIFY` left in code** | Not supported in Spark SQL | Rewrite as subquery + WHERE |
| **Volatile table churn** | `VOLATILE` tables created per session translated as throwaway Delta tables that bloat storage | Use temp views (`createOrReplaceTempView`) or rewrite as DataFrame chain |
| **BTEQ `.IF` / `.GOTO` flow as nested IF in notebook** | Hard to read, error-prone | Decompose into Workflow tasks with `run_if` dependencies |
| **TPT script left as `dbutils.fs.cp` + custom load** | Loses Databricks-native incremental ingestion | Use Auto Loader or `COPY INTO` |
| **Teradata SET-table dedup not preserved** | Original enforced uniqueness; Databricks may silently allow duplicates | Add explicit dedup or PK constraint check |
| **`SAMPLE N` translated as `.limit(N)`** | Spark `.limit()` is deterministic, not random | Use `.orderBy(rand()).limit(N)` or `TABLESAMPLE` |

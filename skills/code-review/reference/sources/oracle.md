# Oracle → Databricks Conversion Reference

Source-specific reference for reviewing Databricks pipelines migrated from Oracle. Load this file in Step 1 if the source system is `oracle`.

---

## 1. Finding Oracle Source Artefacts

Oracle exports typically include:

```
{oracle_root}/
├── ddl/                    *.sql        # CREATE TABLE / VIEW / INDEX
├── packages/               *.pks         # Package specs
│                           *.pkb         # Package bodies (PL/SQL)
├── procedures/             *.prc         # Standalone stored procedures
├── functions/              *.fnc         # Standalone functions
├── triggers/               *.trg         # Database triggers
├── sequences/              *.sql         # CREATE SEQUENCE definitions
├── materialized_views/     *.sql         # Materialized view DDL + refresh logic
├── scripts/                *.sql         # SQL*Plus scripts (often the ETL entry point)
└── jobs/                   *.sql         # DBMS_SCHEDULER job definitions
```

### What to locate for a given Databricks pipeline

- DDL for each target table
- PL/SQL packages/procedures/functions that wrote to the table
- Triggers on the table (often hidden logic — audit columns, validation)
- Sequences used for surrogate keys
- Materialized view refresh logic if the target was an MV
- DBMS_SCHEDULER jobs that orchestrated the loads

### Key Oracle code patterns to grep for

```bash
# Find code writing to a specific table
grep -rln "INSERT INTO.*table_name\|UPDATE.*table_name\|MERGE INTO.*table_name" {oracle_root}/

# Find DDL for a table
find {oracle_root}/ddl -name "*table_name*.sql"

# Find packages mentioning the table
grep -rln "table_name" {oracle_root}/packages/

# Find triggers on the table
grep -rln "ON.*table_name" {oracle_root}/triggers/
```

### PL/SQL package anatomy

```sql
-- Package spec
CREATE OR REPLACE PACKAGE etl_my_table AS
  PROCEDURE load_daily(p_load_date DATE);
  FUNCTION row_count RETURN NUMBER;
END etl_my_table;
/

-- Package body
CREATE OR REPLACE PACKAGE BODY etl_my_table AS
  PROCEDURE load_daily(p_load_date DATE) IS
  BEGIN
    DELETE FROM my_table WHERE load_date = p_load_date;

    INSERT INTO my_table (id, name, load_date)
    SELECT id, name, p_load_date
    FROM source_table
    WHERE etl_status = 'READY';

    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE;
  END load_daily;
END etl_my_table;
/
```

---

## 2. Conversion Fidelity Checks

| Check | What to Compare |
|---|---|
| **DDL → Databricks DDL** | Column list, types, NULLs, defaults preserved |
| **PL/SQL packages → modules / notebooks** | Procedural logic decomposed into SQL transforms or DataFrame operations |
| **Triggers** | Trigger logic re-implemented at the pipeline level (Databricks has no row-level triggers) |
| **Sequences → surrogate keys** | `NEXTVAL` → Databricks `GENERATED ALWAYS AS IDENTITY`, deterministic hash, or sequence table |
| **Materialized views** | Oracle MV with refresh → SDP materialized view or streaming table |
| **DBMS_SCHEDULER jobs** | Scheduled jobs → Databricks Workflow tasks |
| **ROWNUM-based pagination** | `WHERE ROWNUM <= N` → `LIMIT N` or `ROW_NUMBER() OVER (...) <= N` |
| **CONNECT BY hierarchical queries** | `CONNECT BY PRIOR` → recursive CTE (`WITH RECURSIVE`) |
| **Type mapping** | Oracle types → Databricks types — any precision loss? (esp. NUMBER without precision) |
| **DECODE / CASE** | `DECODE(x, v1, r1, v2, r2, default)` → `CASE WHEN` or chained `when/otherwise` |
| **NVL / NVL2 / NULLIF** | NVL → `coalesce`; NVL2 → `case`; NULLIF → `nullif` |
| **Date arithmetic** | `dt + n` (Oracle adds days), `ADD_MONTHS`, `MONTHS_BETWEEN`, `LAST_DAY` → Spark equivalents |
| **Implicit type conversions** | Oracle implicit casts (string → date with NLS_DATE_FORMAT) → make explicit in Spark |
| **DUAL table references** | `SELECT ... FROM DUAL` → `SELECT ...` (no DUAL in Spark) |
| **Partitioning** | RANGE/HASH/LIST partitioning → Databricks partitioning or liquid clustering |
| **Transaction semantics** | `COMMIT` / `ROLLBACK` / `SAVEPOINT` → Delta ACID semantics (no per-session transactions) |

### Detailed checklist

#### DDL
- [ ] All columns from Oracle DDL present in Databricks table
- [ ] Type mapping correct (see §3)
- [ ] `NOT NULL` constraints preserved
- [ ] `DEFAULT` clauses preserved
- [ ] `CHECK` constraints preserved or moved to SDP expectations

#### PL/SQL Procedures
- [ ] Procedural logic decomposed into discrete pipeline steps
- [ ] Cursors / loops removed; replaced with DataFrame/SQL transformations
- [ ] `EXCEPTION` blocks reviewed — `WHEN OTHERS THEN ROLLBACK` patterns don't apply in Delta; use error tables or fail-fast
- [ ] Dynamic SQL reviewed for SQL injection risk
- [ ] OUT parameters / status codes replaced with deterministic returns

#### Sequences
- [ ] `seq.NEXTVAL` calls replaced with:
  - Delta `GENERATED ALWAYS AS IDENTITY` column, OR
  - Deterministic hash (`xxhash64` / `sha2`) on business keys, OR
  - A managed sequence table
- [ ] `monotonically_increasing_id()` NOT used as a primary surrogate key

#### Triggers
- [ ] `BEFORE INSERT/UPDATE` triggers — logic moved into the pipeline transformation, not silently dropped
- [ ] Audit-column triggers (e.g., `created_by`/`created_at`) — implemented as explicit `withColumn` calls
- [ ] Validation triggers — converted to SDP expectations or `quality_rules`

#### Materialized Views
- [ ] Oracle MV refresh mode (`COMPLETE` / `FAST` / `FORCE`) understood:
  - COMPLETE refresh → Databricks materialized view (full recompute)
  - FAST refresh → Streaming table with CDF
- [ ] Refresh schedule preserved in Workflow trigger

#### Hierarchical Queries
- [ ] `CONNECT BY PRIOR` rewritten as recursive CTE:
  ```sql
  -- Oracle
  SELECT employee_id, manager_id, LEVEL
  FROM employees
  START WITH manager_id IS NULL
  CONNECT BY PRIOR employee_id = manager_id;

  -- Databricks (recursive CTE)
  WITH RECURSIVE hierarchy AS (
    SELECT employee_id, manager_id, 1 AS level
    FROM employees WHERE manager_id IS NULL
    UNION ALL
    SELECT e.employee_id, e.manager_id, h.level + 1
    FROM employees e JOIN hierarchy h ON e.manager_id = h.employee_id
  )
  SELECT * FROM hierarchy;
  ```

---

## 3. Type Mapping Reference

| Oracle Type | Databricks Type | Notes |
|---|---|---|
| `NUMBER` (no precision) | `DECIMAL(38, ?)` or `DOUBLE` | Risky — verify expected precision; default may not match |
| `NUMBER(P)` | `DECIMAL(P, 0)` or `BIGINT` / `INT` | If integer-only, use appropriate int type |
| `NUMBER(P, S)` | `DECIMAL(P, S)` | Direct match |
| `BINARY_FLOAT` | `FLOAT` | 32-bit |
| `BINARY_DOUBLE` | `DOUBLE` | 64-bit |
| `VARCHAR2(N)` | `STRING` | Length not enforced — flag if downstream depends on truncation |
| `NVARCHAR2(N)` | `STRING` | UTF-8 by default in Spark |
| `CHAR(N)` | `STRING` | Padding not preserved |
| `CLOB` / `NCLOB` | `STRING` | Large text |
| `BLOB` | `BINARY` | Large binary |
| `RAW(N)` / `LONG RAW` | `BINARY` | |
| `DATE` | `TIMESTAMP` | **Common gotcha** — Oracle DATE includes time, Databricks DATE does not |
| `TIMESTAMP(N)` | `TIMESTAMP` | Spark precision is microseconds |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP` | Spark TIMESTAMP is timezone-aware in UTC |
| `TIMESTAMP WITH LOCAL TIME ZONE` | `TIMESTAMP` | Same as above |
| `INTERVAL YEAR TO MONTH` / `DAY TO SECOND` | `INTERVAL` or compute manually | |
| `ROWID` / `UROWID` | Not migrated | Oracle-specific; no Spark equivalent |
| `XMLTYPE` | `STRING` or `VARIANT` | Store as XML string, parse in pipeline |
| `BFILE` | Not directly migrated | External file pointer — re-implement via Volumes or paths |

**Common gotcha:** Oracle `DATE` includes time-of-day. If migrated to Databricks `DATE`, the time portion is silently dropped. Use `TIMESTAMP` unless you've explicitly confirmed the source date column has no time component.

---

## 4. SQL Function Differences

| Oracle | Spark SQL | Notes |
|---|---|---|
| `NVL(a, b)` | `coalesce(a, b)` or `nvl(a, b)` | Both work in Spark |
| `NVL2(a, b, c)` | `CASE WHEN a IS NOT NULL THEN b ELSE c END` | |
| `DECODE(x, v1, r1, ..., default)` | Chained `CASE WHEN` or `when/otherwise` | |
| `NULLIF(a, b)` | `nullif(a, b)` | Direct match |
| `TO_DATE(s, fmt)` | `to_date(s, fmt)` | Token differences: `YYYY-MM-DD HH24:MI:SS` → `yyyy-MM-dd HH:mm:ss` |
| `TO_CHAR(dt, fmt)` | `date_format(dt, fmt)` | Same token differences |
| `TO_NUMBER(s)` | `cast(s AS DECIMAL(...))` | |
| `SYSDATE` | `current_timestamp()` | Oracle SYSDATE returns DATE (with time); Spark returns TIMESTAMP |
| `SYSTIMESTAMP` | `current_timestamp()` | |
| `TRUNC(dt)` | `date_trunc('day', dt)` | |
| `TRUNC(dt, 'MM')` | `date_trunc('month', dt)` | |
| `ADD_MONTHS(dt, n)` | `add_months(dt, n)` | Direct match |
| `MONTHS_BETWEEN(a, b)` | `months_between(a, b)` | Direct match |
| `LAST_DAY(dt)` | `last_day(dt)` | Direct match |
| `NEXT_DAY(dt, 'MONDAY')` | `next_day(dt, 'Mon')` | Day name format differs |
| `INSTR(s, sub)` | `instr(s, sub)` | Direct match |
| `SUBSTR(s, p, n)` | `substring(s, p, n)` or `substr(s, p, n)` | |
| `LENGTH(s)` | `length(s)` | Direct match |
| `LISTAGG(col, ',') WITHIN GROUP (ORDER BY ...)` | `concat_ws(',', collect_list(col))` over a window or aggregation | No direct LISTAGG; ordering requires window |
| `REGEXP_LIKE(s, p)` | `s RLIKE p` or `regexp(s, p)` | |
| `REGEXP_REPLACE(s, p, r)` | `regexp_replace(s, p, r)` | Direct match |
| `REGEXP_SUBSTR(s, p)` | `regexp_extract(s, p, 0)` | |
| `SYS_GUID()` | `uuid()` | |
| `seq.NEXTVAL` | Identity column or hash key | See §2 |
| `... FROM DUAL` | `SELECT ...` without FROM | No DUAL in Spark |
| `ROWNUM <= N` | `LIMIT N` | But beware — see anti-patterns |
| `ROWNUM <= N` with `ORDER BY` | `ROW_NUMBER() OVER (ORDER BY ...) <= N` | ROWNUM applies BEFORE ORDER BY in Oracle |
| `CONNECT BY ...` | `WITH RECURSIVE ...` | |

---

## 5. Oracle-Era Anti-Patterns to Flag

| Anti-Pattern | Why It's Bad | Fix |
|---|---|---|
| **Oracle `DATE` → Databricks `DATE`** | Oracle DATE includes time; Databricks DATE drops it silently | Use Databricks `TIMESTAMP` unless source has no time component |
| **`NUMBER` without precision** | Oracle NUMBER stores arbitrary precision; mapping to `DOUBLE` loses exactness for monetary values | Use `DECIMAL(P, S)` with verified precision |
| **`seq.NEXTVAL` translated as `monotonically_increasing_id()`** | Not stable across runs or partitions | Use Delta IDENTITY, hash key, or sequence table |
| **PL/SQL cursors translated as Python row loops** | Defeats Spark parallelism | Decompose into DataFrame operations |
| **Triggers silently dropped** | Hidden business logic lost (audit columns, validation, derived columns) | Surface trigger logic explicitly in the pipeline |
| **`WHEN OTHERS THEN ROLLBACK` patterns** | Delta has no per-session transactions; ROLLBACK is a no-op | Use error/quarantine tables or fail-fast |
| **`ROWNUM <= N` translated as `LIMIT N`** | Oracle applies ROWNUM before ORDER BY; LIMIT applies after — semantics differ | Use `ROW_NUMBER() OVER (ORDER BY ...)` and filter |
| **`SELECT ... FROM DUAL` left in** | DUAL doesn't exist in Spark | Remove `FROM DUAL` |
| **Materialized View FAST refresh translated as full recompute** | Loses incremental benefit | Use SDP streaming table with CDF |
| **`TO_DATE` format tokens left as Oracle style** | `YYYY-MM-DD` vs `yyyy-MM-dd` — different in Spark | Convert tokens to Spark style |
| **Oracle implicit casts left implicit** | Spark is stricter; implicit string→date may fail or behave differently | Add explicit `to_date` / `cast` calls |
| **Hierarchical `CONNECT BY` rewritten as Python loop** | Defeats Spark parallelism | Use recursive CTE (`WITH RECURSIVE`) |
| **DBMS_SCHEDULER jobs lifted as cron triggers** | Loses dependency graph and error handling | Use Databricks Workflows with task dependencies |

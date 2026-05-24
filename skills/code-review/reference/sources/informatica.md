# Informatica → Databricks Conversion Reference

Source-specific reference for reviewing Databricks pipelines migrated from Informatica (PowerCenter or IICS/IDMC). Load this file in Step 1 if the user reports the source system as `informatica`.

---

## 1. Finding Informatica Source Artefacts

Informatica exports follow this typical layout:

```
{informatica_root}/
├── mappings/             *.XML        # Mapping definitions (m_*)
├── mapplets/             *.XML        # Reusable transformation groups (mplt_*)
├── workflows/            *.XML        # Workflow definitions (wf_*)
├── sessions/             *.XML        # Session configs (s_*)
├── parameter_files/      *.par        # $$Param / $Var values per env
├── sql/                  *.sql        # Source Qualifier / Pre-SQL / Post-SQL
└── connections/                       # Connection metadata (relational, FF, etc.)
```

IICS/IDMC exports are typically JSON rather than XML, but the same concept tree applies (Mapping, Mapplet, Mapping Task, Taskflow, Parameter, Connection).

### What to locate for a given Databricks pipeline

- The mapping(s) (`m_*`) corresponding to each Databricks table
- Any mapplets (`mplt_*`) referenced by those mappings — they hold reusable logic
- The workflow (`wf_*`) and session (`s_*`) that schedules the mapping
- Parameter files (`*.par`) that supply `$$Param` values per environment
- Pre-SQL / Post-SQL associated with the session

---

## 2. Conversion Fidelity Checks

| Check | What to Compare |
|---|---|
| **Business logic** | Mapping transformations end-to-end — Source Qualifier filter, Expression formulas, Filter/Router conditions, Aggregator group-by + aggregates, Joiner type/condition, Lookup conditions and return ports |
| **Source Qualifier SQL** | Custom SQL or SQL override in SQ → equivalent `spark.sql` / DataFrame; default-generated SQL should match `spark.read.table()` + `.filter()` + `.select()` |
| **Expression transformations** | Per-port expressions (`IIF`, `DECODE`, `LTRIM`, `TO_DATE`, `IS_NUMBER`, etc.) → Spark SQL / `pyspark.sql.functions` equivalents (`when/otherwise`, `coalesce`, `to_date`, etc.) |
| **Filter / Router** | Filter condition preserved verbatim; Router groups mapped to separate writes or filtered DataFrames (default group must be handled) |
| **Joiner** | Same join type (Normal=INNER, Master Outer=RIGHT, Detail Outer=LEFT, Full Outer=FULL); same join condition; "Sorted Input" hint should not silently change semantics |
| **Aggregator** | Same group-by columns, same aggregate functions, same handling of `FIRST`/`LAST` (Informatica-specific — verify ordering is deterministic in Spark) |
| **Lookup** | Lookup condition, return ports, multiple-match policy (Use First/Last/Any/Report Error), connected vs unconnected → broadcast join, MERGE lookup, or `lookup`-style join in Spark |
| **Update Strategy** | `DD_INSERT`/`DD_UPDATE`/`DD_DELETE`/`DD_REJECT` semantics → `MERGE INTO` clauses (whenMatched / whenNotMatched / whenNotMatchedBySource); session "Treat source rows as" overrides preserved |
| **Sequence Generator** | Replace with surrogate-key strategy that is correct under retries (e.g., Delta `IDENTITY`, deterministic hash, or sequence table) — `monotonically_increasing_id()` is NOT a drop-in replacement |
| **Sorter / Rank** | Order-by columns and direction preserved; Rank "Top/Bottom N" + group-by → window function with matching partition/order |
| **Union** | Set semantics — Informatica Union is UNION ALL by default; verify the Spark code is not silently deduping with `union().distinct()` |
| **Normalizer** | Pivot/unpivot logic preserved (occurs-N columns → rows) |
| **SCD Type 1 / Type 2** | Effective-date columns, current-flag, hash compare → MERGE pattern that preserves history identically |
| **Mapping parameters / variables** | `$$Param` and `$Var` mapped to bundle variables, job parameters, or widgets — not hardcoded |
| **Pre-SQL / Post-SQL** | Pre-SQL executed before the target write (notebook cell, job task, or SDP setup); Post-SQL executed after; commit/rollback semantics understood |
| **Null handling** | Informatica treats NULL in string concat as empty; Spark propagates NULL — check `concat` vs `concat_ws` choices |
| **Datatype / precision** | Decimal precision/scale match; high-precision flag handled; date vs timestamp distinction preserved |
| **Date arithmetic** | `ADD_TO_DATE`, `DATE_DIFF`, `LAST_DAY`, `TO_DATE(format)` → Spark equivalents — verify format strings (`MM/DD/YYYY HH24:MI:SS` → `MM/dd/yyyy HH:mm:ss`) |
| **Pushdown optimization** | If Informatica session used full or source-side pushdown, the equivalent should be Spark predicate/projection pushdown to Delta or the federated source — verify with `.explain()` |
| **Connections** | Source/target connections (Oracle, SQL Server, flat file, S3, etc.) → Databricks connections, secrets, or volume paths — no plaintext credentials |
| **Partitioning (session)** | Informatica round-robin/hash/key-range partitioning at session level is an execution detail, NOT a data partitioning choice — do not blindly replicate as `repartition()` |
| **Workflow orchestration** | `wf_*` task ordering, links, decision tasks, and email tasks → Databricks Workflow tasks + dependencies; failure/`$PrevTaskStatus` conditions preserved |
| **Reject / error handling** | Reject rows (`$BadFile`) and row-error logging → equivalent quarantine table or SDP expectation with `ON VIOLATION DROP/FAIL` |
| **Reusable logic** | Mapplets → shared Python modules or shared SDP views — not copy-pasted into every pipeline |

---

## 3. Detailed Per-Transformation Checks

### Source Qualifier (SQ)

- [ ] If the SQ had a **SQL override**, the override is preserved as `spark.sql(...)` or equivalent DataFrame chain (not silently dropped in favour of a default `read.table()`)
- [ ] **Source filter** in SQ properties is preserved as a `.filter()` / `WHERE` clause
- [ ] **User-defined join** in SQ (multi-table SQ) is preserved as a join in Spark
- [ ] **Sorted ports** in SQ (`Number of Sorted Ports`) — verify any downstream code that depended on input order has explicit `.orderBy()` in Spark (Spark does not preserve input order)

### Expression Transformation (EXP)

- [ ] Each port's expression is faithfully translated:
  - `IIF(cond, a, b)` → `when(cond, a).otherwise(b)`
  - `DECODE(x, v1, r1, v2, r2, default)` → chained `when`/`otherwise`
  - `IS_NUMBER`, `IS_DATE`, `IS_SPACES` → equivalent Spark checks (these have no exact equivalent — verify the new logic matches edge cases)
  - `LTRIM`/`RTRIM` with a trim set → `regexp_replace`, not `trim` (Spark `trim` only trims whitespace by default unless you pass `trimStr`)
  - `INSTR`, `SUBSTR` — 1-based indexing in Informatica vs 1-based in Spark SQL (matches), but verify behaviour with negative indices
  - `TO_DATE(str, fmt)` — Informatica format tokens (`MM/DD/YYYY HH24:MI:SS`) must be converted to Spark tokens (`MM/dd/yyyy HH:mm:ss`)
- [ ] Variable ports (`v_*`) that referenced the previous row are not silently translated to row-independent expressions — Spark has no row-by-row state, this needs a window function

**Common violations:**
```python
# BAD: TO_DATE format token left as Informatica style
F.to_date("dt_str", "MM/DD/YYYY")   # Spark interprets DD as day-of-year — wrong

# GOOD: Spark format tokens
F.to_date("dt_str", "MM/dd/yyyy")
```

### Filter / Router

- [ ] Filter condition preserved verbatim (or proven equivalent)
- [ ] **Router** default group is handled — rows that don't match any output group must go somewhere explicit (drop, quarantine, or log)
- [ ] Multiple output groups in a Router → separate writes / separate DataFrames, not a single ambiguous filter

### Joiner (JNR)

- [ ] Join type mapping is correct:
  - Informatica **Normal** → Spark `inner`
  - Informatica **Master Outer** → keep all rows from detail → Spark `right` (or swap sides and use `left`)
  - Informatica **Detail Outer** → keep all rows from master → Spark `left`
  - Informatica **Full Outer** → Spark `full`
- [ ] Join condition uses the same columns and operators
- [ ] "Sorted Input" optimisation in Informatica is an execution hint — it must not have been translated into an `.orderBy()` that changes downstream semantics
- [ ] If the original mapping relied on the master port driving uniqueness, that uniqueness assumption is documented or enforced

### Lookup (LKP)

- [ ] Connected vs unconnected lookup translated appropriately:
  - Connected (returns multiple ports inline) → join or broadcast join
  - Unconnected (called from an expression, returns one value) → join or UDF — verify it's not a per-row driver call
- [ ] Lookup condition preserved exactly
- [ ] **Multiple match policy** preserved: `Use First Value`, `Use Last Value`, `Use Any Value`, `Report Error`, `Use All Values` — each maps to different Spark patterns (window + filter, distinct, exploding join)
- [ ] **Static vs dynamic** lookup cache: dynamic cache that inserts/updates the cache mid-flow → MERGE pattern in Spark, NOT a simple join
- [ ] **Persistent cache** — verify the equivalent cached/broadcast strategy in Spark is correct
- [ ] Default value when lookup misses (`:LKP.lkp_x(...)` returning NULL) — `coalesce` or `when isNull` should fill the same default

### Aggregator (AGG)

- [ ] Group-by columns match
- [ ] Aggregate functions match (`SUM`, `AVG`, `MAX`, `MIN`, `COUNT`, `FIRST`, `LAST`, `MEDIAN`, `PERCENTILE`, `STDDEV`, `VARIANCE`)
- [ ] **`FIRST` / `LAST`** — Informatica returns first/last based on input order; in Spark this requires an explicit `orderBy` plus `first`/`last` with `ignoreNulls`, or a window function — verify ordering is deterministic
- [ ] "Sorted Input" hint not translated into a sort that changes downstream behaviour
- [ ] Group-by on null keys — Informatica groups nulls together; Spark also groups nulls together (matches), but verify if any custom logic relied on this

### Update Strategy (UPD) → MERGE

- [ ] `DD_INSERT` → `whenNotMatched().insertAll()` or `WHEN NOT MATCHED THEN INSERT`
- [ ] `DD_UPDATE` → `whenMatched().updateAll()` or `WHEN MATCHED THEN UPDATE`
- [ ] `DD_DELETE` → `whenMatched().delete()` or `WHEN MATCHED THEN DELETE`
- [ ] `DD_REJECT` → row routed to quarantine table / rejected via SDP expectation `ON VIOLATION DROP`
- [ ] Session-level "Treat source rows as" overrides (`Insert`/`Update`/`Delete`/`Data driven`) are reflected in the MERGE strategy
- [ ] Target update-override SQL preserved if it existed
- [ ] Update Strategy that uses **Insert Else Update** semantics matches the `MERGE` whenMatched/whenNotMatched logic

**Common violations:**
```python
# BAD: Full overwrite replaces an Update Strategy mapping
df.write.mode("overwrite").saveAsTable("target")

# GOOD: MERGE preserves DD_INSERT / DD_UPDATE semantics
target.alias("t").merge(source.alias("s"), "t.key = s.key") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

### Sequence Generator (SEQ)

- [ ] Surrogate key strategy is correct under retries / re-runs:
  - Delta `GENERATED ALWAYS AS IDENTITY` column, OR
  - Deterministic hash key (`xxhash64` / `sha2`) on business keys, OR
  - A sequence table updated transactionally
- [ ] **`monotonically_increasing_id()` is NOT used** as a primary surrogate key — it is not stable across runs or partitions
- [ ] If the Informatica `SEQ` had `Reset` enabled per session, the Spark equivalent matches (usually a hash, not a sequence)

### Sorter (SRT) / Rank (RNK)

- [ ] Sort columns + direction preserved (only where downstream actually depends on order — Sorter is sometimes vestigial)
- [ ] Rank "Top N" / "Bottom N" with group-by → `row_number()`/`rank()` window with matching partition + order, filtered `<= N`

### Union (UN)

- [ ] Informatica Union is **UNION ALL** by default — the Spark code should be `df1.unionByName(df2)`, NOT `df1.union(df2).distinct()`
- [ ] `unionByName(..., allowMissingColumns=True)` only when the original schemas differed and that was acceptable

### Normalizer (NRM)

- [ ] Pivot-to-rows logic (occurs-N columns expanded to N rows) preserved
- [ ] Generated key column equivalent in Spark (often `posexplode` or a manual stack)

### Stored Procedure / SQL Transformation

- [ ] Stored procedures called from mappings have a Databricks equivalent (SQL function, notebook task, or rewritten logic) — not silently dropped
- [ ] SQL transformations executing per-row queries → join or broadcast join, NOT a per-row UDF call

### SCD Type 1 / Type 2

- [ ] **Type 1**: target overwrites in place — verify MERGE updates the same row, no history
- [ ] **Type 2**: effective-from / effective-to / current-flag columns preserved
  - Closing the prior current row (set `eff_to`, `current_flag=false`)
  - Inserting the new current row (`eff_from = now`, `current_flag=true`)
  - Hash compare on change-detect columns matches the Informatica change-detect logic exactly

### Mapping Parameters / Variables

- [ ] `$$Param` values (per parameter file) → bundle variables, job parameters, or widgets — not hardcoded
- [ ] Mapping variables that **persisted across runs** (`$$LastRunDate` updated by `SETVARIABLE`) need explicit state in Spark — usually a small Delta "control" table or a job parameter passed in from a previous task
- [ ] System variables (`$SessStartTime`, `$$$SessStartTime`) replaced with deterministic equivalents (`current_timestamp()` or a job parameter)

### Pre-SQL / Post-SQL

- [ ] **Pre-SQL** statements executed before the target write — either as a notebook cell, a separate workflow task, or an SDP setup hook
- [ ] **Post-SQL** statements executed after the target write — same options
- [ ] Commit / rollback semantics understood — Databricks tables (Delta) do not have the same per-session transaction model; review any logic that depended on Informatica session-level commits

### Workflow / Session

- [ ] Workflow task dependencies (`$prev.Status = SUCCEEDED`, condition links) → Databricks Workflow task dependencies + `run_if` conditions
- [ ] Email tasks → Databricks Job notifications, alerts, or webhook tasks
- [ ] Command tasks (shell scripts) → notebook tasks or job tasks running scripts
- [ ] Decision tasks → conditional task / `if/else` task
- [ ] Worklet calls (`wl_*`) → reusable Databricks Workflow components or shared job definitions
- [ ] Session-level overrides (connection, parameter file path) reflected in the Databricks job parameters per environment

### Reject / Error Handling

- [ ] Bad-file (`$BadFile`) rows have an equivalent quarantine table or SDP expectation with `ON VIOLATION DROP ROW`
- [ ] Row-level error log table (`PMERR_*`) → equivalent error/quarantine Delta table, OR a documented decision to drop without logging
- [ ] "Stop on errors" session setting → equivalent fail-fast behaviour in the Databricks job

### Datatypes / Precision

- [ ] Decimal precision and scale preserved; high-precision flag respected
- [ ] Date vs Timestamp distinction preserved (Informatica `date/time` is a single type; Spark distinguishes `DATE` and `TIMESTAMP`)
- [ ] String length truncation: Informatica enforces port length; Spark does not — verify downstream consumers don't depend on truncation, or add an explicit truncate
- [ ] Null vs empty-string handling: Informatica's `CONCAT` treats NULL as empty; Spark `concat` returns NULL on any NULL input — use `concat_ws` to mimic Informatica behaviour

### Pushdown Optimization

- [ ] If the Informatica session used **full** or **source-side pushdown**, the Spark equivalent is predicate + projection pushdown into Delta or the federated source
- [ ] Verify with `df.explain()` that filters and projections are pushed down to the scan
- [ ] If the original ran inside the source database (full pushdown), document whether the new Spark job is now scanning what used to be evaluated in the DB

### Partitioning

- [ ] Informatica **session-level** partitioning (round-robin, hash, key-range, database) is an execution detail — it is NOT a data design choice and should NOT be blindly replicated as `repartition()`
- [ ] Target **table** partitioning in Spark is chosen based on data size, query patterns, and skew — not copied from the Informatica session config
- [ ] No accidental `repartition(N)` calls that match Informatica session partition counts without justification

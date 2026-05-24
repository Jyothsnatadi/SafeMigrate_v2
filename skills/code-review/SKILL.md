---
name: code-review
description: Deep code review of Databricks pipelines migrated from external sources (AWS Glue, Redshift, Informatica PowerCenter/IICS/IDMC, etc.). Covers Databricks best practices, cost efficiency, DAB configuration, code quality, testing, and source-to-Databricks conversion fidelity. Produces a structured review report with severity-rated findings and an email summary. Works with both SDP (Spark Declarative Pipelines) and PySpark job workloads.
user-invocable: true
---

# Pipeline Code Review Workflow

This skill performs a deep code review of Databricks pipelines that have been migrated from an external source system (AWS Glue, Redshift, Informatica, etc.). It assesses code quality, Databricks best practices, cost efficiency, and conversion fidelity against the original source.

**Announce at start:** "I'm using the code-review skill to review your migrated Databricks pipeline code."

## Overview: The Review Framework

```
1. SETUP                -> Identify the pipeline code, source system, and original source code
2. EXPLORE              -> Map pipeline structure, sub-pipelines, and domains
3. CODE REVIEW          -> Best practices, cost, config, quality, testing (source-agnostic)
4. CONVERSION COMPARE   -> Side-by-side conversion fidelity check (source-specific)
5. REPORT               -> Severity-rated findings + email summary
```

**Reference files:**
- `reference/review-checklist.md` — Source-agnostic checklist for all Databricks review dimensions
- `reference/repo-structure.md` — Databricks target-side repo layout + sparse-checkout guidance
- `reference/sources/{source}.md` — Source-specific details; load only the one matching the user's source system

---

## Step 1: Setup

### Gather Required Information

Ask the user for the following:

| Input | Description | Example |
|---|---|---|
| **Pipeline path(s)** | Path(s) to the Databricks pipeline code in the repo | `aviation/databricks_pipeline/insight_hub/aviation_ih_ops_mi_layer_34` |
| **Source system** | What was this migrated from? | `glue`, `redshift`, `informatica`, `teradata`, `oracle` |
| **Source flavor** (if Informatica) | PowerCenter or IICS/IDMC? | `powercenter`, `iics`, `idmc` |
| **Original source path** | Path to the original source code/exports | `aviation/aviation/use_cases/.../custom/` or `informatica_exports/finance/fin_gl/` |
| **Business domain** | Domain for locating original source code | `aviation`, `midstream`, `castrol`, `finance` |
| **Branch** (optional) | If the code is on a feature branch, which one? | `feature/migr/aviation/ops_mi_sprint_2` |

If a branch is specified, use `git show origin/<branch>:<path>` and `git ls-tree` to read files without checking out or creating worktrees. If no branch is specified, read directly from the working directory.

### Load the Source-Specific Reference

**Before proceeding to Step 4, Read the matching source reference file:**

| Source system | Reference file to Read |
|---|---|
| `glue` / `redshift` / both | `reference/sources/aws-glue-redshift.md` |
| `informatica` | `reference/sources/informatica.md` |
| `teradata` | `reference/sources/teradata.md` |
| `oracle` | `reference/sources/oracle.md` |

The source reference contains:
- Where the source code lives (paths, file conventions, search patterns)
- Per-transformation conversion fidelity checks specific to that source
- Source-specific anti-patterns to flag during Step 3

### Locate the Code

**Databricks pipeline code** — typical layout:

```
{domain}/databricks_pipeline/{sub_domain}/{pipeline_name}/
├── databricks.yml
├── resources/*.yml
├── src/dlt/*_dlt_pipeline.py        # SDP pipelines
├── src/jobs/*.py                    # PySpark jobs
├── tests/
└── notebooks/
```

See `reference/repo-structure.md` for the full Databricks-side directory map, federated catalog naming, and sparse-checkout setup.

**Original source code** — paths depend on the source system. See the matching `reference/sources/{source}.md`.

---

## Step 2: Explore Pipeline Structure

List all files in the pipeline directory and map the structure:

- Identify sub-pipelines and their layouts
- Categorise into domains/layers (SDLF, data_prep, use_cases, staging, conform, curated, mart, etc.)
- Note the workload type for each: SDP (`from pyspark import pipelines as dp`) vs PySpark job (`spark.read`/`spark.write`)
- Identify the DAB configuration files (`databricks.yml`, resource YAMLs)
- Identify tests and QA notebooks
- On the source side, list the source artefacts (Glue jobs, Informatica mappings/workflows, etc.) that this pipeline is meant to replace — see the source reference file for what to look for

Present the structure summary to the user before proceeding:

```
Pipeline: {pipeline_name}
Workload type: {SDP / PySpark / Mixed}
Sub-pipelines found: {N}
  - {sub_pipeline_1} (SDP, 3 tables, 2 views)
  - {sub_pipeline_2} (PySpark, 5 tasks)
Source artefacts to compare: {N} (e.g., glue-xxx.py / m_gl_load mapping)
Test coverage: {unit tests found / not found}
DAB config: {databricks.yml + N resource YAMLs}
```

---

## Step 3: Code Review

Review all code files across these dimensions. See `reference/review-checklist.md` for the full checklist.

### 3a. Databricks Best Practices

| Check | What to Look For |
|---|---|
| `import dlt` → `pyspark.pipelines` **(HIGH)** | `import dlt` must be replaced with `from pyspark import pipelines as dp`. All `@dlt.table` must be `@dp.materialized_view` (batch reads) or `@dp.table` (streaming reads). `dlt.read()`/`dlt.read_stream()` must be replaced with `spark.read.table()`/`spark.readStream.table()`. |
| SDP vs PySpark | Is SDP used where appropriate? Note CI/CD constraints if the team can't use SDP yet. |
| Incremental processing | Are large tables using streaming tables or MERGE, or wastefully doing full overwrites? |
| Data quality | Are SDP Expectations or custom assertions in place? |
| Auto Loader | Is Auto Loader used for file-based ingestion, or manual file listing? |
| Streaming vs batch | Is the right mode chosen for each data source? |
| Schema handling | Is schema evolution handled (`mergeSchema`, `rescuedDataColumn`)? |
| CDF enablement | Is `delta.enableChangeDataFeed` set on tables that downstream consumers read incrementally? |

### 3b. Cost Efficiency

| Check | What to Look For |
|---|---|
| `display()` / `.show()` **(HIGH)** | `display()` or `.show()` in production pipeline code — fails in non-interactive job/SDP context, must be removed |
| Column pruning | `SELECT *` or `spark.table()` without selecting specific columns |
| Unnecessary `.count()` | `.count()` calls that trigger full scans just for logging |
| Unpartitioned window functions | `ROW_NUMBER()` / `RANK()` without `PARTITION BY` — single-partition shuffle |
| Repartition misuse | `.repartition()` without good reason — expensive shuffle |
| Full overwrite on large tables | `write.mode("overwrite")` where MERGE or partition overwrite would suffice |
| Table creation patterns | Dynamic table names per run vs proper partitioned/append tables |
| Read amplification | Reading full source tables without filter pushdown |

Source-specific cost checks (e.g., Informatica lookup re-reads, Glue DPU-style sizing assumptions) are in the source reference file.

### 3c. DAB Configuration

| Check | What to Look For |
|---|---|
| Bundle variables | `${bundle.target}` is correct; `${bundle.environment}` is NOT valid |
| `run_as` | Should use service principal identity, not personal accounts |
| Compute config | If `serverless: true`, missing cluster config is fine. If serverless, check `performance_target` (prefer STANDARD over default PERFORMANCE_OPTIMIZED unless justified). Otherwise check sizing. |
| Variable definitions | Do variables in `databricks.yml` match what the pipeline code references? |
| Timeout | Is a timeout configured? Missing timeout = potential runaway cost. |
| Environment configs | Are dev/nonprod/prod targets properly differentiated? |
| Permissions | Are grants/permissions configured for each target environment? |
| Source parameter mapping | Source-system parameters (e.g., Glue env vars, Informatica `$$Param` / parameter file values) mapped to DAB variables or job parameters — see source reference for details. |

### 3d. Code Quality

| Check | What to Look For |
|---|---|
| Filter logic | AND vs OR errors, tautologies (`x > 0 OR x <= 0`), contradictions |
| Hardcoded values | Catalog names, schema names, dates, connection strings that should be parameterised via bundle variables or secrets |
| Code duplication | Copy-pasted logic across sub-pipelines that should be shared |
| Dead code | Unused imports, commented-out blocks, unreachable code paths, source-side ports/columns carried through unused |
| Naming | Consistent naming conventions across tables, columns, functions |
| Error handling | Bare `except: pass` swallowing errors silently |
| SQL injection | String interpolation in SQL without parameterisation (rare but check) |

### 3e. Testing

| Check | What to Look For |
|---|---|
| Unit test coverage | Are there tests? Do they test actual logic or just `assert True`? |
| Placeholder tests | `assert True`, `pass`, or empty test bodies = no real coverage |
| Validation scripts | Do QA/validation notebooks actually check results or just print them? |
| Exception handling in tests | Are test failures raised or swallowed? |
| Test data | Are tests using realistic test data or trivially empty DataFrames? |
| Reconciliation against source | Are there row-count or column-hash parity checks against the original source output? |

---

## Step 4: Source Conversion Comparison

Find the original source code and compare it side-by-side with the Databricks implementation.

**Use the source-specific reference loaded in Step 1** — it contains the per-transformation conversion checks for that source system:

- **Glue / Redshift** → `reference/sources/aws-glue-redshift.md`
- **Informatica** → `reference/sources/informatica.md`
- **Teradata** → `reference/sources/teradata.md`
- **Oracle** → `reference/sources/oracle.md`

Each source reference covers:
- How to locate the originals (file conventions, search patterns)
- Per-transformation conversion fidelity checks
- Source-specific anti-patterns

### What Improved vs What Was Lost (universal)

Track both sides:
- **Improvements:** Better typing, incremental processing (MERGE/streaming), SDP expectations, version-controlled code instead of XML/scripts, cheaper compute, cleaner code
- **Losses:** Dropped columns/ports, changed semantics, missing source-side configs, weaker error handling, lost lineage that the source system used to provide

---

## Step 5: Report

Generate three outputs.

### 5a. Code Review Report (source-agnostic)

```
CODE REVIEW REPORT
==================
Pipeline: {pipeline_name}
Branch: {branch}
Date: {today}
Source system: {glue / redshift / informatica}
Reviewer: Genie Code (automated)

SUMMARY
-------
Total findings: {N}
  HIGH: {N}
  MEDIUM: {N}
  LOW: {N}
  INFO: {N}

FINDINGS BY CATEGORY
---------------------

## Databricks Best Practices

### [HIGH] {title}
- **File:** {filepath}:{line_number}
- **Finding:** {description}
- **Recommendation:** {what to do}

### [MEDIUM] {title}
...

## Cost Efficiency
## DAB Configuration
## Code Quality
## Testing
## Curation Framework Standards

PRIORITISED RECOMMENDATIONS
----------------------------
1. {Highest impact fix}
2. {Second highest}
3. {Third}
```

### 5b. Conversion Review Report

```
CONVERSION REVIEW REPORT
=========================
Pipeline: {pipeline_name}
Source: {glue / redshift / informatica}
Date: {today}

GAP SUMMARY
------------
| Area | Status | Notes |
|------|--------|-------|
{One row per Conversion Fidelity Check area you reviewed in Step 4.
Areas come from the source reference's checks (e.g., Business logic,
Type casting, Joiner semantics, Update Strategy, etc.).
Status: MATCH / GAP / CHANGED / LOSSY / RISKY / DIVERGENT / PRESERVED / MISSING.}

ARTEFACT-BY-ARTEFACT COMPARISON
--------------------------------
{One section per source artefact. Unit depends on source:
  Glue        → sub-pipeline / Glue job file
  Redshift    → table / DDL file
  Informatica → mapping (m_*), mapplet (mplt_*), or workflow (wf_*)}

### {artefact_1}
| Source artefact | Databricks target | Logic match | Notes |
|---|---|---|---|
| {source_file_or_artefact} | {databricks_file} | YES/NO/PARTIAL | {details} |

### {artefact_2}
...

WHAT IMPROVED
--------------
- {improvement_1}
- {improvement_2}

WHAT WAS LOST OR CHANGED
--------------------------
- {loss_1}
- {loss_2}

APPENDIX: SOURCE MAPPING
--------------------------
| Databricks Pipeline | Source Original |
|---|---|
| {databricks_path} | {source_path} |
```

### 5c. Email Summary

```
REVIEW EMAIL SUMMARY
=====================

Subject: Code Review — {pipeline_name} ({branch})

Hi team,

{Brief positive assessment of what's working — 2-3 sentences}

**Items to address before production:**
1. {concise finding}
2. {concise finding}

**Items to address before cutover:**
1. {concise finding}
2. {concise finding}

**General code quality findings:**
- {finding}
- {finding}

**What went well:**
- {positive observation}
- {positive observation}

Best regards,
{automated review}
```

---

## Severity Definitions

| Severity | Meaning | Action |
|---|---|---|
| **HIGH** | Will cause incorrect data, job failures, or significant cost waste in production | Must fix before production deployment |
| **MEDIUM** | Suboptimal but functional — technical debt, minor cost inefficiency, missing best practices | Should fix before cutover, can deploy to dev/nonprod |
| **LOW** | Style, naming, minor improvements — won't cause issues | Nice to have, address when convenient |
| **INFO** | Observations, context, or known constraints that aren't issues | No action required |

---

## Tone Guidelines

- Professional and measured — flag issues clearly but don't exaggerate
- Use severity levels (HIGH/MEDIUM/LOW) not dramatic language
- Acknowledge known constraints (e.g., CI/CD limitations for SDP, serverless for cluster config, source-system behaviours that have no exact Spark equivalent)
- Lead with what's working before diving into issues
- Keep the email summary concise and actionable

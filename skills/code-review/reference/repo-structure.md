# dataworx_cc Repository Structure (Databricks side)

Guide to the Databricks target-side layout in the `dataworx_cc` repo, federated catalog naming, and sparse-checkout setup. For source-side (Glue, Redshift, Informatica, etc.) repo layout, see `sources/{source}.md`.

---

## Top-Level Domains

The repo is organized by business domain at the top level:

```
dataworx_cc/
├── aviation/           # Aviation fuels & insight hub
├── midstream/          # Midstream oil & gas
├── castrol/            # Castrol lubricants
├── candp/              # Customers & Products
├── fleet/              # Fleet management
├── futuremob/          # Future mobility
├── advanced_pricing/   # CIS pricing
├── cmp/                # Castrol Marketing Platform
├── gipp/               # Global Integrated Planning
├── appliedscience/     # Data science / ML
├── dataworx/           # Shared platform / cross-domain
└── dwxengineering/     # Platform engineering
```

---

## Finding Databricks SDP (Spark Declarative Pipelines) Code

Databricks pipelines follow this pattern:

```
{domain}/databricks_pipeline/{sub_domain}/{pipeline-name}/
├── databricks.yml                          # Bundle config (catalogs, schemas, targets)
├── resources/
│   ├── {pipeline-name}_dlt_bundle.yml      # SDP pipeline definitions
│   └── {pipeline-name}_job_bundle.yml      # PySpark job definitions (if any)
├── src/dlt/
│   └── {pipeline-name}_dlt_pipeline.py     # Main SDP notebook
├── src/jobs/
│   └── *.py                                # PySpark job scripts
├── notebooks/
│   ├── generic_history_load.py             # History backfill
│   └── qa_*.py                             # QA notebooks (if created)
└── tests/
    └── unit_test.py
```


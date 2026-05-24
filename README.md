# SafeMigrate_v2
catch issues early. Deliver confidence in every migration


## Installation

### Workspace-scoped (recommended — available to all users)

Copy the `.assistant/skills/` directory to your Databricks workspace:

```
Workspace/
  .assistant/
    skills/
      best-practices-and-cost-review/
      code-review/

```

Use the Databricks UI (Import), the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html), or the Workspace API:

```bash
# Using Databricks CLI
databricks workspace import-dir .assistant/skills /Workspace/.assistant/skills --overwrite
```

### User-scoped (personal use only)

```bash
databricks workspace import-dir .assistant/skills /Users/<your-email>/.assistant/skills --overwrite
```

### Verify installation

In a Genie Code session (Agent mode), type:

```
@code-review
Do a code review of my informatica to databricks pipeline . original code is at ... and converted at ...
```

If the skill responds with a routing recommendation, installation is working.

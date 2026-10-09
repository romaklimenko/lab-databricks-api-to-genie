# Weather agent lab

This repository implements a supervisor with ingestion, stewardship, and review agents.
Read README.md before changing behavior. Keep the offline replay independent of cloud access.

- Use Python 3.12 and uv. Commit uv.lock.
- Run `uv run pytest` and `uv run ruff check .` before committing.
- Keep identifiers and credentials in environment variables, never committed values.
- Public fixtures must include their source, retrieval date, licence, and hash.
- Agent output is data. Render executable code from the checked-in pipeline template.
- A deterministic validator must accept artifacts before Git or cloud writes.
- Restrict writes to the configured lab schema, job, Genie Agent, and repository branch.
- Preserve phase receipts. Repeated requests must not repeat completed external effects.
- Use databricks-core, databricks-jobs, databricks-serverless, and databricks-genie-agents skills when available.
- Update README.md when behavior, configuration, or verification results change.

CLAUDE.md contains only the include for this file.

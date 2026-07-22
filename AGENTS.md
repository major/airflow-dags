# airflow-dags

DAG bundle for a homehosted Apache Airflow deployment. Airflow's `GitDagBundle`
clones this repo directly and loads DAGs from `dags/`. There is no build,
test suite, or CI in this repo; a small lint/format/type-check toolchain
(ruff + pyright) runs locally via pre-commit.

## Deployment model

- Push to `main` — Airflow's DAG processor picks up changes on its next
  refresh automatically. No deploy step, no PR merge gate, no packaging.
- There is no local Airflow install — DAGs cannot be run locally. Correctness
  comes from writing valid Airflow code, reviewed by eye and gated by
  pre-commit (ruff + pyright).

## Local checks (pre-commit)

Every commit runs hooks defined in `.pre-commit-config.yaml`:

- `pre-commit-hooks` — trailing whitespace, EOF newline, YAML/TOML syntax,
  large-file guard, mixed line endings, etc.
- `ruff` (lint with `--fix`, then `ruff format`) — config in
  `pyproject.toml` under `[tool.ruff]`.
- `pyright` — config in `pyproject.toml` under `[tool.pyright]`, set to
  `typeCheckingMode = "basic"`.

One-time setup (host must have `pre-commit` available — e.g.
`pip install pre-commit` or `pipx install pre-commit`):

    pre-commit install

Run on demand without committing:

    pre-commit run --all-files

Run a single tool directly for faster iteration:

    ruff check --fix dags/
    ruff format dags/
    pyright dags/

### Why pyright doesn't fail on missing `airflow` imports

The pyright config sets `reportMissingImports = "none"` because the Airflow
SDK and provider packages are not installed in this repo's dev env — this
is a GitDagBundle, not a packaged project, and there is intentionally no
runtime install. Type checks run on what is resolvable (`requests`,
stdlib, etc.); the `airflow` types are left to be validated at runtime by
Airflow itself.

## Code conventions (from existing DAGs)

- Import from `airflow.sdk` (Airflow 3 TaskFlow API: `DAG`, `task`, `Param`,
  `BaseHook`, `get_current_context`), not the legacy `airflow.operators.*` /
  `airflow.models` style. See `dags/discord_test_message.py`.
- Every DAG file starts with `from __future__ import annotations` and a
  module docstring explaining what it does and any secret it needs.
- `schedule=None`, `catchup=False`, fixed `start_date=datetime.datetime(2024, 1, 1)`
  are the norm for these manually-triggered DAGs.

## Secrets

- **Never hardcode credentials in a DAG.** Secrets live outside this repo, in
  `~/git/major/homehosted` (SOPS-encrypted k8s Secret
  `apps/airflow/connections-secrets.sops.yaml`, name `airflow-connections`),
  injected into Airflow containers via `extraEnvFrom`.
- Each secret key is `AIRFLOW_CONN_<CONN_ID>`, a JSON connection string
  Airflow reads directly as a Connection (no `airflow connections add`).
- Pattern used by `discord_test_message`: DAG takes a `conn_id` Param
  (default `discord_webhook`), reads `BaseHook.get_connection(conn_id).password`
  at task run time. Adding a new target (e.g. another webhook) means adding a
  new `AIRFLOW_CONN_*` key in the homehosted repo and passing that `conn_id`
  when triggering — no code change here.
- To edit that secret: `sops apps/airflow/connections-secrets.sops.yaml` in
  the homehosted repo, then commit/push (Flux reconciles it).

## Keeping this file current

When a change substantially alters layout, conventions, secrets handling, or
deployment flow (e.g. adding tooling, CI, a package manifest, or a new DAG
pattern), update this `AGENTS.md` in the same change rather than letting it
go stale.

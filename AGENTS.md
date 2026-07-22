# airflow-dags

DAG bundle for a homehosted Apache Airflow deployment. Airflow's `GitDagBundle`
clones this repo directly and loads DAGs from `dags/` — there is no build,
package manifest, test suite, lint config, or CI in this repo.

## Deployment model

- Push to `main` — Airflow's DAG processor picks up changes on its next
  refresh automatically. No deploy step, no PR merge gate, no packaging.
- There is nothing to run locally to "test" a DAG; correctness comes from
  writing valid Airflow code and reviewing it by eye (no local Airflow
  install/lint/test tooling is set up in this repo).

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

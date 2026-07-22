# airflow-dags

DAG bundle repository for the [homehosted](https://github.com/major/homehosted) Apache Airflow deployment.

Airflow's [DAG bundles](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html)
feature clones this repository via `GitDagBundle`, tracking the `main` branch
and loading DAGs from the `dags/` subdirectory.

## Layout

- `dags/` — DAG definition files loaded by Airflow.

## Adding a DAG

1. Add a new Python file under `dags/`.
2. Commit and push to `main`.
3. The Airflow DAG processor picks up changes automatically on its next
   refresh interval; no deployment or redeploy is required.

## Secrets

DAGs must never hardcode credentials. This repo only holds DAG code —
secrets live in the `~/git/major/homehosted` GitOps repo as a SOPS-encrypted
Kubernetes Secret (`apps/airflow/connections-secrets.sops.yaml`, name
`airflow-connections`), wired into every Airflow container via
`extraEnvFrom` in `apps/airflow/helm/helmrelease.yaml`. Each key in that
secret is an `AIRFLOW_CONN_<CONN_ID>` JSON connection string, which Airflow
reads directly as a Connection — no `airflow connections add` needed.

Example: `discord_test_message` reads a Discord incoming webhook URL from
the `discord_webhook` connection (env key `AIRFLOW_CONN_DISCORD_WEBHOOK`,
value `{"conn_type": "http", "password": "<webhook url>"}`), via the DAG's
`conn_id` param (default `discord_webhook`).

To add another webhook (e.g. a second channel), in the homehosted repo:

```bash
sops apps/airflow/connections-secrets.sops.yaml   # edit in place, decrypts/re-encrypts
```

Add a new key, e.g. `AIRFLOW_CONN_DISCORD_WEBHOOK_ALERTS`, with the same
JSON shape. Commit and push; Flux reconciles the secret and the Helm
release automatically. Trigger `discord_test_message` with
`conn_id=discord_webhook_alerts` to use it, or reuse the same connection id
convention in a new DAG.

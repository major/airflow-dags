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

### stockcharts_discord_alerts

The `stockcharts_discord_alerts` DAG polls StockCharts for new alerts every
5 minutes (Monday–Friday) and sends them to Discord. It requires two pieces
of configuration:

1. **Connection**: `AIRFLOW_CONN_STOCKCHARTS_DISCORD_WEBHOOK` (env key in
   `airflow-connections` secret, same JSON shape as `discord_webhook`). The
   password field may contain multiple comma-separated webhook URLs; the DAG
   posts to each one, best-effort, logging and continuing past per-webhook
   failures.

2. **Variable**: `stockcharts_last_successful_run` (Airflow Variable, created
   and managed automatically by the DAG itself). This tracks the timestamp of
   the last successful poll, so the DAG only sends new alerts. If missing, the
   DAG defaults to the last 5 minutes. No manual setup is required, but you
   may see it in the Airflow UI's Variables list.

To set up the connection in the homehosted repo:

```bash
sops apps/airflow/connections-secrets.sops.yaml
```

Add a new key `AIRFLOW_CONN_STOCKCHARTS_DISCORD_WEBHOOK` with value
`{"conn_type": "http", "password": "<webhook url>"}` (or multiple URLs
comma-separated). Commit and push; Flux reconciles the secret automatically.

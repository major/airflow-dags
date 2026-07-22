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

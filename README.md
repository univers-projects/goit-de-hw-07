# Airflow DAG: Olympic medal alerts

The DAG in `dags/olympic_medal_alerts_dag.py` performs the homework pipeline:

```text
create_table -> pick_medal -> pick_medal_task
                                  |-> calc_Bronze --|
                                  |-> calc_Silver --|-> generate_delay -> check_for_correctness
                                  `-> calc_Gold   --|
```

## What it does

1. Creates `hw07_medal_results` if it does not exist. The table contains `id`, `medal_type`, `count`, and `created_at`.
2. Randomly picks `Bronze`, `Silver`, or `Gold`.
3. Uses `BranchPythonOperator` to run exactly one matching `calc_*` task.
4. The selected MySQL task counts the records in `olympic_dataset.athlete_event_results` and inserts the result.
5. `generate_delay` waits for the configured duration.
6. `check_for_correctness` is an SQL sensor that succeeds only if the newest result is no more than 30 seconds old.

## Local Docker setup

The project contains a local MySQL and Airflow environment. Download the source dataset once:

```bash
./download_dataset.sh
docker compose up -d --build
```

Airflow UI is available at [http://localhost:8080](http://localhost:8080). Log in with:

```text
username: admin
password: admin
```

The `AIRFLOW_CONN_GOIT_MYSQL_DB` environment variable creates the `goit_mysql_db` connection automatically. It points at the MySQL container and has access to:

- the writable `olympic_dataset.hw07_medal_results` table;
- the source `olympic_dataset.athlete_event_results` table loaded from CSV.

The DAG does not run on a schedule. Trigger it manually from Airflow UI.

## Demonstrate success and sensor failure

- Start a normal DAG run with the default parameter `delay_seconds: 5`. The SQL sensor succeeds.
- Trigger another run with **Run configuration**:

```json
{"delay_seconds": 35}
```

The insert is then older than 30 seconds when `check_for_correctness` starts. The sensor polls for 15 seconds and finishes as **failed**. This is the required failed-sensor example.

## Evidence to submit

1. In Airflow Grid/Graph view, capture a successful run: one `calc_*` task is green and the other two are skipped.
2. Capture the MySQL table `hw07_medal_results` with rows containing medal type, calculated count, and timestamp.
3. Run with `delay_seconds: 35` and capture the red `check_for_correctness` task in Airflow UI.

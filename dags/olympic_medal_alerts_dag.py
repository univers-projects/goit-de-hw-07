"""Airflow DAG for calculating Olympic medal counts and checking a fresh result."""

from __future__ import annotations

import random
import time
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

MYSQL_CONN_ID = "goit_mysql_db"
RESULTS_TABLE = "hw07_medal_results"
SOURCE_TABLE = "olympic_dataset.athlete_event_results"
MEDALS = ("Bronze", "Silver", "Gold")


def pick_medal() -> str:
    """Return a randomly chosen medal; Airflow stores it in XCom automatically."""
    medal = random.choice(MEDALS)
    print(f"Selected medal: {medal}")
    return medal


def pick_medal_task(ti, **_) -> str:
    """Return the task ID that should run for the chosen medal."""
    medal = ti.xcom_pull(task_ids="pick_medal")
    if medal not in MEDALS:
        raise ValueError(f"Unexpected medal in XCom: {medal!r}")
    return f"calc_{medal}"


def generate_delay(**context) -> None:
    """Delay the sensor; override delay_seconds in the DAG run configuration."""
    delay_seconds = int(
        context["dag_run"].conf.get(
            "delay_seconds", context["params"]["delay_seconds"]
        )
    )
    if delay_seconds < 0:
        raise ValueError("delay_seconds must be non-negative")
    print(f"Waiting {delay_seconds} seconds before freshness check.")
    time.sleep(delay_seconds)


with DAG(
    dag_id="olympic_medal_alerts",
    description="Calculate one random Olympic medal count and validate its freshness.",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["goit", "homework", "olympic", "mysql"],
    params={
        "delay_seconds": Param(
            5,
            type="integer",
            minimum=0,
            description="Seconds to wait before the freshness sensor. Use 35 to show failure.",
        )
    },
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10) NOT NULL,
                `count` INT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    select_medal = PythonOperator(task_id="pick_medal", python_callable=pick_medal)

    branch_by_medal = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=pick_medal_task,
    )

    calculate_tasks = []
    for medal in MEDALS:
        calculate_tasks.append(
            SQLExecuteQueryOperator(
                task_id=f"calc_{medal}",
                conn_id=MYSQL_CONN_ID,
                sql=f"""
                    INSERT INTO {RESULTS_TABLE} (medal_type, `count`, created_at)
                    SELECT '{medal}', COUNT(*), NOW()
                    FROM {SOURCE_TABLE}
                    WHERE medal = '{medal}';
                """,
            )
        )

    delay = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30
            FROM {RESULTS_TABLE};
        """,
        poke_interval=5,
        timeout=15,
        mode="poke",
    )

    create_table >> select_medal >> branch_by_medal >> calculate_tasks
    calculate_tasks >> delay >> check_for_correctness

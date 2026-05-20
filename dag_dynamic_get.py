from __future__ import annotations

from datetime import datetime
from typing import Dict

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

# Generate 10 DAGs dynamically with top-level Variable.get
DAG_COUNT = 10

def _print_var(key: str, value: str):
    print(f"Dynamic GET {key} length:", len(value))


def _step(message: str):
    print(message)


def _build_dag(dag_id: str, var_key: str) -> DAG:
    # top-level Variable.get at module load time for each DAG
    top_value = Variable.get(var_key)

    with DAG(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["cpu-test", "vars", "dynamic"],
    ) as dag:
        start = EmptyOperator(task_id="start")
        show = PythonOperator(
            task_id="show_top_level_var",
            python_callable=_print_var,
            op_args=[var_key, top_value],
        )
        step_1 = PythonOperator(task_id="step_1", python_callable=_step, op_args=["step_1 executed"])
        step_2 = PythonOperator(task_id="step_2", python_callable=_step, op_args=["step_2 executed"])
        done = PythonOperator(task_id="done", python_callable=_step, op_args=["done"])

        start >> show >> step_1 >> step_2 >> done

    return dag


for i in range(1, DAG_COUNT + 1):
    dag_name = f"dyn_get_{i}"
    var_name = f"dyn_get_var_{i}"
    globals()[dag_name] = _build_dag(dag_name, var_name)

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

# Generate 10 DAGs dynamically with top-level Variable.set
DAG_COUNT = 10


def _set_var(key: str, value: str):
    Variable.set(key, value)
    print(f"Dynamic SET {key} done")


def _step(message: str):
    print(message)


def _build_dag(dag_id: str, var_key: str) -> DAG:
    # top-level Variable.set at module load time for each DAG
    top_value = (
        '{"source":"dynamic_set","dag_id":"%s","payload":{"items":20,"notes":"medium payload"}}'
        % dag_id
    )
    Variable.set(var_key, top_value)

    with DAG(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["cpu-test", "vars", "dynamic"],
    ) as dag:
        start = EmptyOperator(task_id="start")
        step_1 = PythonOperator(task_id="step_1", python_callable=_step, op_args=["step_1 executed"])
        set_in = PythonOperator(
            task_id="set_in_dag_var",
            python_callable=_set_var,
            op_args=[var_key, top_value],
        )
        step_2 = PythonOperator(task_id="step_2", python_callable=_step, op_args=["step_2 executed"])
        done = PythonOperator(task_id="done", python_callable=_step, op_args=["done"])

        start >> step_1 >> set_in >> step_2 >> done

    return dag


for i in range(1, DAG_COUNT + 1):
    dag_name = f"dyn_set_{i}"
    var_name = f"dyn_set_var_{i}"
    globals()[dag_name] = _build_dag(dag_name, var_name)

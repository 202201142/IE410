from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

# Top-level Variable.get (module import time)
TOP_LEVEL_VAR_2 = Variable.get("dag2_feature_flags_blob")


def _print_top_level_var():
    print("DAG2 top-level var length:", len(TOP_LEVEL_VAR_2))


def _echo_message(message: str):
    print(f"DAG2 says: {message}")


def _print_done():
    print("DAG2 done")


with DAG(
    dag_id="dag_2",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["cpu-test", "vars"],
) as dag:
    # Variable.get inside DAG context
    in_dag_var_2 = Variable.get("dag2_partner_config_blob")

    start = EmptyOperator(task_id="start")
    show_top = PythonOperator(task_id="show_top_level_var", python_callable=_print_top_level_var)
    show_in = PythonOperator(
        task_id="show_in_dag_var",
        python_callable=lambda: print("DAG2 in-dag var length:", len(in_dag_var_2)),
    )
    step_1 = PythonOperator(task_id="step_1", python_callable=_echo_message, op_args=["step_1 executed"])
    done = PythonOperator(task_id="done", python_callable=_print_done)

    start >> [show_top, show_in] >> step_1 >> done

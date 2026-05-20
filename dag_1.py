from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

# Top-level Variable.get (module import time)
TOP_LEVEL_VAR_1 = Variable.get("dag1_customer_profile_blob")


def _print_top_level_var():
    print("DAG1 top-level var length:", len(TOP_LEVEL_VAR_1))


def _print_context(**context):
    print("DAG1 run_id:", context.get("run_id"))


def _print_static():
    print("DAG1 static task executed")


def _print_done():
    print("DAG1 done")


with DAG(
    dag_id="dag_1",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["cpu-test", "vars"],
) as dag:
    # Variable.get inside DAG context
    in_dag_var_1 = Variable.get("dag1_processing_rules_blob")

    start = EmptyOperator(task_id="start")
    show_top = PythonOperator(task_id="show_top_level_var", python_callable=_print_top_level_var)
    show_in = PythonOperator(
        task_id="show_in_dag_var",
        python_callable=lambda: print("DAG1 in-dag var length:", len(in_dag_var_1)),
    )
    show_ctx = PythonOperator(task_id="show_context", python_callable=_print_context)
    done = PythonOperator(task_id="done", python_callable=_print_done)

    start >> [show_top, show_in] >> show_ctx >> done

from dateutil import parser
from datetime import datetime, timedelta
from datetime import timezone
from pendulum import datetime
from airflow import DAG
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator
from airflow.operators.python_operator import PythonOperator
import logging
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import ( SQLColumnCheckOperator, SQLTableCheckOperator, SQLCheckOperator,)
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

_CONN_ID = "cdw-hive"
# Define default arguments for the DAG
default_args = {
    'owner': 'frothkoe',
    'start_date':datetime(2024, 4, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'cdw-lab8-demo',
    default_args=default_args,
    description='A DAG to execute SQL query and capture results',
    schedule_interval='@daily',  # Run the DAG daily
    catchup=False,
    is_paused_upon_creation=False
)


column_checks = SQLColumnCheckOperator(
        task_id="column_checks",
	dag = dag,
        conn_id=_CONN_ID,
        table="airlinedata.airports_ice",
        column_mapping={
            "iata": {
                "null_check": {"equal_to": 0},
                "distinct_check": {"geq_to": 2},
            },
        },
    )

table_checks = SQLTableCheckOperator(
        task_id="table_checks",
	dag = dag,
        conn_id=_CONN_ID,
        table="airlinedata.airports_ice",
        checks={
            "row_count_check": {"check_statement": "COUNT(*) >= 10000"},
        },
    )

column_checks >>  table_checks


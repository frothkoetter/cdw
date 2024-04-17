from airflow import DAG
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cloudera.cdp.airflow.operators.cdw_operator import CDWOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import logging


# Define default arguments for the DAG
default_args = {
    'owner': 'frothkoetter',
    'start_date': datetime(2024, 4, 15),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'run_sql_simple',
    default_args=default_args,
    description='A DAG for executing SQL queries and performing error checks',
    schedule_interval="@daily", 
    is_paused_upon_creation=False
)

# Define the SQL query 
query = """
select 
  * 
from 
  airlinedata.airports_ice limit 10;
"""

run_sql = SQLExecuteQueryOperator(
        task_id='run_sql',
        conn_id='cdw-impala',  # Specify your cdw connection ID
        sql=query,
	dag=dag
    )

run_sql

from dateutil import parser
from datetime import datetime, timedelta
from datetime import timezone
from airflow import DAG
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.operators.python_operator import PythonOperator
import logging

default_args = {
    'owner': 'frothkoe',
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
    'start_date':datetime(2024, 4, 16),
}

dag = DAG(
    'cdw-lab7-demo',
    default_args=default_args, 
    schedule_interval='@daily', 
    catchup=False, 
    is_paused_upon_creation=False
)
# Define SQL query to execute
sql_query = """
SELECT COUNT(*) AS total_records FROM airlinedata.airports_ice;
"""

# Function to execute SQL query and set XCom variable
def execute_sql_and_set_xcom_variable(**kwargs):
    ti = kwargs['ti']

    # Execute SQL query using HiveOperator
    hive_task = SQLExecuteQueryOperator(
        task_id='execute_sql_task',
        conn_id='cdw-impala',  # Specify your Hive connection ID
        sql=sql_query,
        dag=dag,
    )

    # Wait for HiveOperator task to complete
    hive_task.execute(context=kwargs)

    # Retrieve SQL query result (total_records)
    sql_result = ti.xcom_pull(task_ids='execute_sql_task', key='return_value')

    logging.info("-----> retrive SQL result.")
    logging.info( sql_result )
 

    # Set SQL result as XCom variable
    if sql_result:
        total_records = sql_result['total_records']
        ti.xcom_push(key='total_records_xcom', value=total_records)
        print(f"Set XCom variable 'total_records_xcom' with value: {total_records}")
    else: 
        print(f"no SQL query result")

# Define PythonOperator to execute the task function
set_xcom_variable_task = PythonOperator(
    task_id='set_xcom_variable_task',
    python_callable=execute_sql_and_set_xcom_variable,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
set_xcom_variable_task

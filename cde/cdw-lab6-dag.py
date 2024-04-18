from dateutil import parser
from datetime import datetime, timedelta
from datetime import timezone
from airflow import DAG
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator
from airflow.providers.common.sql.operators.sql import ( 
	BranchSQLOperator,
	SQLColumnCheckOperator, 
	SQLTableCheckOperator, 
	SQLCheckOperator,
	SQLValueCheckOperator,
	SQLExecuteQueryOperator)

# Define custom fetch handler function to process query results
def process_query_results(cursor, **kwargs):
    results = cursor.fetchall()  # Fetch all rows from the cursor
    for row in results:
        # Process each row (e.g., log row data)
        print(f"Row Data: {row}")

default_args = {
    'owner': 'frothkoe',
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
    'start_date':datetime.now(),
}

dag = DAG(
    'airflow-pipeline-demo',
    default_args=default_args, 
    schedule_interval='@daily', 
    catchup=False, 
    is_paused_upon_creation=False
)

_CONN_ID="cdw-impala"

cdw_check_quotation_mark = """
select
      count(*) as failures
    from (
with validation as (
	select airport as field
	from default.airports
),
validation_errors as (
	select field from validation
	where field rlike('\"')
)
select *
from validation_errors
) quotation_marks_test;
"""

dw_check_quotation_mark = BranchSQLOperator(
    task_id="dataset-check-quotation_mark",
    conn_id=_CONN_ID,
    follow_task_ids_if_false=['dataset-check-num-rows'],
    follow_task_ids_if_true=['dataset-qa-quotation_mark'],
    sql=cdw_check_quotation_mark,
    dag=dag,
)

cdw_qa_quotation_mark = """
update default.airports
set airport = regexp_replace( airport ,'"','')
where airport rlike('"');
"""
dw_qa_quotation_mark = SQLExecuteQueryOperator(
    task_id="dataset-qa-quotation_mark",
    conn_id=_CONN_ID,
    sql=cdw_qa_quotation_mark,
    dag=dag,
)

cdw_check_num_rows = """
select count(1) as num_rows from airlinedata.airports_ice;
"""

dw_check_num_rows = SQLCheckOperator(
    task_id="dataset-check-num-rows",
    conn_id=_CONN_ID,
    sql=cdw_check_num_rows,
    trigger_rule="none_failed",
    dag=dag,
)

cdw_create = """
drop table if exists default.airports;
create table default.airports 
 stored by iceberg TBLPROPERTIES('format-version'='2') 
as 
 select * from airlinedata.airports_ice;
update default.airports
set airport = regexp_replace( airport ,'"','')
where airport rlike('"') and 1=2;
"""

dw_create = SQLExecuteQueryOperator(
    task_id="dataset-create-cdw",
    conn_id=_CONN_ID,
    sql=cdw_create,
    split_statements=True,
    return_last=True,
    dag=dag,
)

cdw_query = """
select * from default.airports limit 10;
"""

dw_query = SQLExecuteQueryOperator(
    task_id="dataset-query-cdw",
    conn_id=_CONN_ID,
    sql=cdw_query,
    dag=dag,
    show_return_value_in_logs=True
)
dw_cursor = SQLExecuteQueryOperator(
    task_id="dataset-cursor-cdw",
    conn_id=_CONN_ID,
    sql=cdw_query,
    dag=dag,
    show_return_value_in_logs=True,
    handler=process_query_results
)
dw_column_checks = SQLColumnCheckOperator(
        task_id="dw_column_checks",
        dag = dag,
        conn_id=_CONN_ID,
        table="default.airports",
        column_mapping={
            "iata": {
                "null_check": {"equal_to": 0},
                "unique_check": {"equal_to": 0},
                "distinct_check": {"geq_to": 2},
            },
        },
    )

dw_table_checks = SQLTableCheckOperator(
        task_id="dw_table_checks",
        dag = dag,
        conn_id=_CONN_ID,
        table="default.airports",
        checks={
            "row_count_check": {"check_statement": "COUNT(*) between 3000 and 4000"   },
        },
    )


dw_create >> dw_table_checks >>  dw_check_num_rows >> dw_check_quotation_mark >> dw_qa_quotation_mark >>  dw_column_checks >>  dw_query >> dw_cursor

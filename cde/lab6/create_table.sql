drop table if exists airflow_job_metrics;
create table airflow_job_metrics ( 
ts string, file_line string, log_level string, message string, test_time date, database_tested string, model_tested string) 
stored by iceberg;

insert into airlinedata_qa.airflow_job_metrics(ts, log_level, file_line, test_time, model_tested, message, database_tested) 
values (cast("2024-06-11T14:07:55.998000Z" as timestamp),
'INFO',"taskinstance.py:1159",
cast('2024-06-11T14:07:55.998000Z' as date),
'airlinedata',
'Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: airflow-pipeline-demo.dataset-check-num-rows cde-job-run-20 [queued]>',
'airlinedata')



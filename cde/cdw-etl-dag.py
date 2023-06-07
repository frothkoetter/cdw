from airflow import DAG
from datetime import datetime, timedelta
from cloudera.cdp.airflow.operators.cdw_operator import CDWOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'frothkoetter',
    'depends_on_past': False,
    'email': ['frothkoetter@cloudera.com'],
    'start_date': datetime(2021,1,1,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'cdw-etl-dag', default_args=default_args, catchup=False, schedule_interval="15 * * * *", is_paused_upon_creation=False)

vw_airlinedata_cleanup = """
DROP MATERIALIZED VIEW airlinedata_ws.traffic_cancel_airport;
drop table if exists airlinedata_ws.airlines;
drop table if exists airlinedata_ws.airports;
drop table if exists airlinedata_ws.planes;
drop table if exists airlinedata_ws.flights;
"""

vw_airlinedata_dim = """
CREATE DATABASE IF NOT EXISTS airlinedata_ws;
create table airlinedata_ws.airlines as select * from airlinedata.airlines_csv;
create table airlinedata_ws.airports as select * from airlinedata.airports_csv;
create table airlinedata_ws.planes as select * from airlinedata.planes_csv;
"""

vw_airlinedata_fact = """
CREATE DATABASE IF NOT EXISTS airlinedata_ws;
create table airlinedata_ws.flights partitioned by (month) as select month, dayofmonth, dayofweek, deptime, crsdeptime, arrtime, crsarrtime, uniquecarrier, flightnum, tailnum, actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, origin, dest, distance, taxiin, taxiout, cancelled, cancellationcode, diverted, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from airlinedata.flights_csv;
"""

vw_airlinedata_mv = """
CREATE MATERIALIZED VIEW airlinedata_ws.traffic_cancel_airport as SELECT flights.cancelled, airlines.description, airlines.code AS code, flights.month AS month FROM airlinedata_ws.flights flights JOIN airlinedata_ws.airlines airlines ON (flights.uniquecarrier = airlines.code);
"""

vw_airlinedata_report = """
SELECT
  SUM(flights.cancelled) AS num_flights_cancelled,
  SUM(1) AS total_num_flights,
  MIN(airlines.description) AS airline_name,
  airlines.code AS airline_code
FROM
  airlinedata_ws.flights flights
  JOIN airlinedata_ws.airlines airlines ON (flights.uniquecarrier = airlines.code)
GROUP BY
  airlines.code
ORDER BY
  num_flights_cancelled DESC LIMIT 100;
"""

start = DummyOperator(task_id='start', dag=dag)

cdw_cleanup = CDWOperator(
    task_id='cdw-cleanup',
    dag=dag,
    cli_conn_id='cdw',
    hql=vw_airlinedata_cleanup,
    schema='default',
    use_proxy_user=False,
    query_isolation=True

)

cdw_dim = CDWOperator(
    task_id='cdw-dim',
    dag=dag,
    cli_conn_id='cdw',
    hql=vw_airlinedata_dim,
    schema='default',
    use_proxy_user=False,
    query_isolation=True

)

cdw_fact = CDWOperator(
 task_id='cdw-fact',
   dag=dag,
   cli_conn_id='cdw',
   hql=vw_airlinedata_fact,
   schema='default',
   use_proxy_user=False,
   query_isolation=True

)

wait = DummyOperator(task_id='wait', dag=dag)

cdw_mv = CDWOperator(
    task_id='cdw-mv',
    dag=dag,
    cli_conn_id='cdw',
    hql=vw_airlinedata_mv,
    schema='default',
    use_proxy_user=False,
    query_isolation=True

)
cdw_report = CDWOperator(
    task_id='cdw-report',
    dag=dag,
    cli_conn_id='cdw',
    hql=vw_airlinedata_report,
    schema='default',
    use_proxy_user=False,
    query_isolation=True

)
end = DummyOperator(task_id='end', dag=dag)

start >> cdw_cleanup >> [cdw_dim, cdw_fact] >> wait >> cdw_mv >> cdw_report >> end

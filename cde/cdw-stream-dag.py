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
    'cdw-stream-dag', default_args=default_args, catchup=False, schedule_interval="30 * * * *", is_paused_upon_creation=False)

vw_airlinedata_init = """
create table if not exists flights_final
(
 year int, month int, dayofmonth int,
 dayofweek int, deptime int, crsdeptime int, arrtime int,
 crsarrtime int, uniquecarrier string, flightnum int, tailnum string,
 actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int,
 depdelay int, origin string, dest string, distance int, taxiin int,
 taxiout int, cancelled int, cancellationcode string, diverted string,
 carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, lateaircraftdelay int,
 origin_lon string,origin_lat string, dest_lon string,dest_lat string,
 prediction decimal,proba  decimal,prediction_delay  decimal,
 temp decimal, pressure decimal,humidity decimal,wind_speed decimal, clouds string,
 batch_id BIGINT )
stored by ICEBERG;
create table if not exists flights_batch_offset(
 batch_id bigint DEFAULT SURROGATE_KEY(), run_ts timestamp,
 from_ts bigint,
 to_ts bigint,
 row_count bigint);
"""

vw_airlinedata_tmp = """
drop  table if exists flights_streaming__tmp;
create table flights_streaming__tmp
(
 year int, month int, dayofmonth int,
 dayofweek int, deptime int, crsdeptime int, arrtime int,
 crsarrtime int, uniquecarrier string, flightnum int, tailnum string,
 actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int,
 depdelay int, origin string, dest string, distance int, taxiin int,
 taxiout int, cancelled int, cancellationcode string, diverted string,
 carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, lateaircraftdelay int,
 origin_lon string,origin_lat string, dest_lon string,dest_lat string,
 prediction decimal,proba  decimal,prediction_delay  decimal,
 temp decimal, pressure decimal,humidity decimal,wind_speed decimal, clouds string);
"""

vw_airlinedata_transform = """
CREATE DATABASE IF NOT EXISTS airlinedata_ws;
with flights as ( select
   year, month, dayofmonth, dayofweek, deptime, crsdeptime, arrtime, crsarrtime, uniquecarrier, flightnum, tailnum,
   actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, origin, dest, cast( distance as integer ) as distance, taxiin, taxiout,
   cancelled, cancellationcode, diverted, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay,
   origin_lon, origin_lat, cast( dest_lon as float) as dest_lon, cast(dest_lat as float) as dest_lat,
   cast( translate( substr(prediction, instr(prediction,'prediction=')+11,1 ),'}','') as integer),
   cast( translate( substr(prediction, instr(prediction,'proba=')+6,4 ),'}','') as float),
   cast( translate( substr(prediction, instr(prediction,'prediction_delay=')+17,2 ),',','') as integer) ,
   cast( translate( substr( weather_json, instr(weather_json,'temp=')+5,5 ),',','') as float) ,
   cast( translate( substr( weather_json, instr(weather_json,'pressure=')+9,6 ),',','') as float) ,
   cast( translate( substr( weather_json, instr(weather_json,'humidity=')+9,2 ),',','') as float) ,
   cast( translate( substr( weather_json, instr(weather_json,'speed=')+6,5 ),',','') as float) ,
   cast( translate( substr( weather_json, instr(weather_json,'all=')+4,3 ),'}','') as float)
  from
   airlinedata.flights_streaming_ice
   ),
offset as ( select max(to_ts) as max_ts from flights_batch_offset)
insert  into flights_streaming__tmp
   select
     flights.*
   from
    flights, offset
    where  
      unix_timestamp(concat( year,'-', month, '-', dayofmonth, ' ' ,
        substring(lpad(deptime,4,'0'),1,2),':', substring(lpad(deptime,4,'0'),3,2) ,':00' )) > nvl(max_ts,0);
"""

vw_airlinedata_offset = """
insert into flights_batch_offset(run_ts,from_ts,to_ts,row_count)
 select
     current_timestamp(),
     min( unix_timestamp(concat( year,'-', month, '-', dayofmonth, ' ' ,
       substring(lpad(deptime,4,'0'),1,2),':', substring(lpad(deptime,4,'0'),3,2) ,':00' ))),
     max( unix_timestamp(concat( year,'-', month, '-', dayofmonth, ' ' ,
       substring(lpad(deptime,4,'0'),1,2),':', substring(lpad(deptime,4,'0'),3,2) ,':00' ))),
     count(1)
from
 flights_streaming__tmp;
"""

vw_airlinedata_sweep = """
with row_ingest as ( select * from flights_streaming__tmp),
     offset as ( select max(batch_id) from flights_batch_offset)
insert into flights_final
select
 r.*,
 b.*
from
 row_ingest r,
 offset b;
--
select count(1) as rows_in_destination from flights_final;
select sum(row_count) as processed_rows_total from flights_batch_offset;
select * from flights_batch_offset;
"""

start = DummyOperator(task_id='start', dag=dag)

cdw_init = CDWOperator(
    task_id='cdw-init',
    dag=dag,
    cli_conn_id='cdw',
    hql=vw_airlinedata_init,
    schema='airlinedata_ws',
    use_proxy_user=False,
    query_isolation=True

)

cdw_tmp = CDWOperator(
    task_id='cdw-tmp',
    dag=dag,
    cli_conn_id='cdw',
    hql=vw_airlinedata_tmp,
    schema='airlinedata_ws',
    use_proxy_user=False,
    query_isolation=True

)

cdw_transform = CDWOperator(
 task_id='cdw-transform',
   dag=dag,
   cli_conn_id='cdw',
   hql=vw_airlinedata_transform,
   schema='airlinedata_ws',
   use_proxy_user=False,
   query_isolation=True

)

wait = DummyOperator(task_id='wait', dag=dag)

cdw_offset = CDWOperator(
    task_id='cdw-offset',
    dag=dag,
    cli_conn_id='cdw',
    hql=vw_airlinedata_offset,
    schema='airlinedata_ws',
    use_proxy_user=False,
    query_isolation=True

)
cdw_sweep = CDWOperator(
    task_id='cdw-sweep',
    dag=dag,
    cli_conn_id='cdw',
    hql=vw_airlinedata_sweep,
    schema='airlinedata_ws',
    use_proxy_user=False,
    query_isolation=True

)
end = DummyOperator(task_id='end', dag=dag)

start >> cdw_init >> cdw_tmp >> cdw_transform >> cdw_offset >> cdw_sweep >> end

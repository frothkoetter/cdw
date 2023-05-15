create database airlinedata;
use airlinedata;

drop table if exists flights_streaming_ice;
create table flights_streaming_ice
( year string, month string, dayofmonth string,
 dayofweek string, deptime string,crsdeptime string, arrtime string,
 crsarrtime string, uniquecarrier string, flightnum string, tailnum string,
 actualelapsedtime string, crselapsedtime string, airtime string, arrdelay string,
  depdelay string, origin string, dest string, distance string, taxiin string,
 taxiout string, cancelled string,cancellationcode string, diverted string,
 carrierdelay string, weatherdelay string, nasdelay string, securitydelay string,
lateaircraftdelay string, origin_lon string,origin_lat string, dest_lon string,dest_lat string,
prediction string, proba string, weather_json string
 )
stored by
 ICEBERG;

 drop view flights_streaming_ice_cve;
 create view flights_streaming_ice_cve as
 select
   year, month, dayofmonth, dayofweek, deptime, crsdeptime, arrtime, crsarrtime, uniquecarrier, flightnum, tailnum,
   actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, origin, dest, cast( distance as integer ) as distance, taxiin, taxiout,
   cancelled, cancellationcode, diverted, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay,
   origin_lon, origin_lat, cast( dest_lon as float) as dest_lon, cast(dest_lat as float) as dest_lat,
   cast( translate( substr(prediction, instr(prediction,'prediction=')+11,1 ),'}','') as float) as prediction,
   cast( translate( substr(prediction, instr(prediction,'proba=')+6,4 ),'}','') as float) as proba,
   case translate( substr(prediction, instr(prediction,'prediction=')+11,1 ),'}','')
     when 1 then trunc(rand() * 99 + 1) end as prediction_delay ,
   cast( translate( substr( weather_json, instr(weather_json,'temp=')+5,5 ),',','') as float) as  temp,
   cast( translate( substr( weather_json, instr(weather_json,'pressure=')+9,6 ),',','') as float) as  presssure,
   cast( translate( substr( weather_json, instr(weather_json,'humidity=')+9,2 ),',','') as float) as  humidity,
   cast( translate( substr( weather_json, instr(weather_json,'speed=')+6,5 ),',','') as float) as  wind_speed,
   cast( translate( substr( weather_json, instr(weather_json,'all=')+4,3 ),'}','') as float) as clouds
 from
   flights_streaming_ice;


select
 *
from
 flights_streaming_ice_cve
limit 10;




# CDW Workshops - vHoL  

Analyze Stored Data

## Introduction
This workshop gives you an overview of how to use the Cloudera Data Warehouse service to quickly explore raw data, create curated versions of the data for reporting and dashboarding, and then scale up usage of the curated data by exposing it to more users. It highlights the performance and automation capabilities that help ensure performance is maintained while controlling cost.  

Enitiy-Relation Diagram of tables we use in todays workshop: 
- fact table: flights (86mio rows) 
- dimension tables: airlines (1.5k rows), airports (3.3k rows) and planes (5k rows)

![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.002.png)

## Lab Setup



You have to set your **workload password**.

- [x] Click on username in the bottom left
- [x] Click Profile
- [x] Click “Set Workload Password” link
- [x] Then enter and confirm the password
- [x] Click “**Set Workload Password”**


-----
## Lab 1 - Create Database
*Do all these steps as the* **“db\_user001”..”db\_user020”** *unless otherwise noted.*

Navigate to Data Warehouse, then Virtual Warehouse and open the SQL Authoring tool DAS or HUE.
 
Create new database for your user to be used, or use one that is already created for you.

```sql
-- Change *** of database name
CREATE DATABASE DB_USER0**;
USE DB_USER0**;

```
Your can check your current database 
```sql
select current_database();
```
-----
## Lab 2 - External Tables

Run DDL to create four external tables on the CSV data files, which are already in cloud object storage.

```sql
drop table if exists flights_csv;
CREATE EXTERNAL TABLE flights_csv(month int, dayofmonth int, 
 dayofweek int, deptime int, crsdeptime int, arrtime int, 
 crsarrtime int, uniquecarrier string, flightnum int, tailnum string, 
 actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int, 
 depdelay int, origin string, dest string, distance int, taxiin int, 
 taxiout int, cancelled int, cancellationcode string, diverted string, 
 carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, 
lateaircraftdelay int) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION '/airlinedata-csv/flights' tblproperties("skip.header.line.count"="1");

drop table if exists planes_csv;
CREATE EXTERNAL TABLE planes_csv(tailnum string, owner_type string, manufacturer string, issue_date string, model string, status string, aircraft_type string, engine_type string, year int) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION '/airlinedata-csv/planes' tblproperties("skip.header.line.count"="1");

drop table if exists airlines_csv;
CREATE EXTERNAL TABLE airlines_csv(code string, description string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION '/airlinedata-csv/airlines' tblproperties("skip.header.line.count"="1");

drop table if exists airports_csv;
CREATE EXTERNAL TABLE airports_csv(iata string, airport string, city string, state DOUBLE, country string, lat DOUBLE, lon DOUBLE) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION '/airlinedata-csv/airports' tblproperties("skip.header.line.count"="1");

drop table if exists unique_tickets;
CREATE external TABLE unique_tickets_csv (ticketnumber BIGINT, leg1flightnum BIGINT, leg1uniquecarrier STRING, leg1origin STRING,   leg1dest STRING, leg1month BIGINT, leg1dayofmonth BIGINT,   
 leg1dayofweek BIGINT, leg1deptime BIGINT, leg1arrtime BIGINT,   
 leg2flightnum BIGINT, leg2uniquecarrier STRING, leg2origin STRING,   
 leg2dest STRING, leg2month BIGINT, leg2dayofmonth BIGINT,   leg2dayofweek BIGINT, leg2deptime BIGINT, leg2arrtime BIGINT ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION '/airlinedata-csv/unique_tickets' 
tblproperties("skip.header.line.count"="1");

```


Check that you created tables

```sql
SHOW TABLES;
```


Results


|TAB_NAME|
| :- |
|airlines_csv|
|airports_csv|
|flights_csv|
|planes_csv|
|unique_tickets_csv|


Run exploratory queries to understand the data. This reads the CSV data, converts it into a columnar in-memory format, and executes the query.

QUERY: Airline Delay Aggregate Metrics by Airplane

DESCRIPTION: Customer Experience Reporting showing airplanes that have the highest average delays, causing the worst customer experience.

*Do all these steps in the* **“db\_user001”..”db\_user020”** *unless otherwise noted.*

```sql
SELECT tailnum,
       count(*),
       avg(depdelay) AS avg_delay,
       max(depdelay),
       avg(taxiout),
       avg(cancelled),
       avg(weatherdelay),
       max(weatherdelay),
       avg(nasdelay),
       max(nasdelay),
       avg(securitydelay),
       max(securitydelay),
       avg(lateaircraftdelay),
       max(lateaircraftdelay),
       avg(airtime),
       avg(actualelapsedtime),
       avg(distance)
FROM flights_csv
GROUP BY tailnum
ORDER BY avg_delay DESC;
```
Note: Runing the first time may take a clouple minutes.

Results

|TAILNUM	| _C1|	AVG_DELAY|	_C3| _C4 |	_C5	| _C6 | _C7 | _C8 | _C9 | _C10	|_C11	| _C12	| _C13	| _C14 | _C15 |	_C16 |
| :- | :- |:- |:- |:- |:- |:- |:- |:- |:- |:- |:- |:- |:- |:- |:- |:- |
|N702AW	|1	|null	|null	|0.0	|1.0	|0.0 |0 | 0.0 |0 |0.0| 0 |0.0 |0 |null |null |843.0 |
N662??	| 1	|null	|null	|0.0	|1.0	|null |null |null	| null	|null	|null	|null	|null	|null |null	|528.0|
|N043BR	|1	|null	|null	|0.0	|1.0	|0.0	|0	|0.0 |0	|0.0	|0	|0.0	|0	|null	|null	|224.0|


QUERY: Engine Types Causing Most Delays
DESCRIPTION: Ad Hoc Exploration to Investigate - Exploratory query to determine which engine type contributes to the most delayed flights.


```sql
SELECT model,
       engine_type
FROM planes_csv
WHERE planes_csv.tailnum IN
    (SELECT tailnum
     FROM
       (SELECT tailnum,
               count(*),
               avg(depdelay) AS avg_delay,
               max(depdelay),
               avg(taxiout),
               avg(cancelled),
               avg(weatherdelay),
               max(weatherdelay),
               avg(nasdelay),
               max(nasdelay),
               avg(securitydelay),
               max(securitydelay),
               avg(lateaircraftdelay),
               max(lateaircraftdelay),
               avg(airtime),
               avg(actualelapsedtime),
               avg(distance)
        FROM flights_csv
        WHERE tailnum IN ('N194JB',
                          'N906S',
                          'N575ML',
                          'N852NW',
                          'N000AA')
        GROUP BY tailnum) AS delays);

```
NOTE: If this returns no results, then remove the 'WHERE tailnum in …' clause

Results


|MODEL	|ENGINE_TYPE|
| :- | :- |
|A330-223	|Turbo-Fan| 

-----
## Lab 3 - Managed Tables

Run “CREATE TABLE AS SELECT” queries to create full ACID ORC type of the tables. This creates curated versions of the data which are optimal for BI usage.

*Do all these steps in the* **“db\_user001”..”db\_user020”** *unless otherwise noted.*

```sql
drop table if exists airlines_orc;
create table airlines_orc as select * from airlines_csv;

drop table if exists airports_orc;
create table airports_orc as select * from airports_csv;

drop table if exists planes_orc;
create table planes_orc as select * from planes_csv;

drop table if exists unique_tickets_orc;
create table unique_tickets_orc as select * from unique_tickets_csv;

drop table if exists flights_orc;
create table flights_orc partitioned by (month) as 
select month, dayofmonth, dayofweek, deptime, crsdeptime, arrtime, crsarrtime, uniquecarrier, flightnum, tailnum, actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, origin, dest, distance, taxiin, taxiout, cancelled, cancellationcode, diverted, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay 
from flights_csv;

```

This takes a few minutes!

Check that you created managed & external tables

```sql
USE DB_USER0**;
SHOW TABLES;
```

Results


|TAB_NAME|
| :- |
|airlines_csv|
|airlines_orc|
|airports_csv|
|airports_orc|
|flights_csv|
|flights_orc|
|planes_csv|
|planes_orc|
|unique_tickets_csv|
|unique_tickets_orc|



Experiment with different queries to see effects of the data cache on each executor.

Run query. Highlight both “SET …” and “SELECT …” when you execute.

```sql
SET hive.query.results.cache.enabled=false;

SELECT
  SUM(flights.cancelled) AS num_flights_cancelled,
  SUM(1) AS total_num_flights,
  MIN(airlines.description) AS airline_name,
  airlines.code AS airline_code
FROM
  flights_orc flights
  JOIN airlines_orc airlines ON (flights.uniquecarrier = airlines.code)
GROUP BY
  airlines.code
ORDER BY
  num_flights_cancelled DESC;
```

Go to Queries page, then click on the query that just ran, then scroll down to DAG INFO,
choose DAG COUNTERS, then filter for 'cache', then show CACHE_MISS_BYTES and/or CACHE_HIT_BYTES. 
Take note of the query run time too.

Note:  sometimes it takes up to a few minutes for DAS to parse the query metrics and expose them in the UI.

Run query again.
Check the cache metrics again to see the improved hit rate.


Query to find all international flights: flights where destination airport country is not the same as origin airport country

```sql
SELECT DISTINCT 
   flightnum, 
   uniquecarrier, 
   origin, 
   dest, 
   month, 
   dayofmonth, 
   `dayofweek`
FROM 
  flights_orc f, 
   airports_orc oa, 
   airports_orc da  
WHERE 
   f.origin = oa.iata 
   and f.dest = da.iata 
   And oa.country <> da.country 
ORDER BY
   month ASC, 
   dayofmonth ASC;
```

Query to explore passenger manifest data:  do we have international connecting flights?

```sql
SELECT * FROM 
  unique_tickets_orc a, 
  flights_orc o, 
  flights_orc d,
  airports_orc oa, 
  airports_orc da  
WHERE
   a.leg1flightnum = o.flightnum
   AND a.leg1uniquecarrier = o.uniquecarrier 
   AND a.leg1origin = o.origin 
   AND a.leg1dest = o.dest 
   AND a.leg1month = o.month 
   AND a.leg1dayofmonth = o.dayofmonth
   AND a.leg1dayofweek = o.`dayofweek` 
   AND a.leg2flightnum = d.flightnum
   AND a.leg2uniquecarrier = d.uniquecarrier 
   AND a.leg2origin = d.origin 
   AND a.leg2dest = d.dest 
   AND a.leg2month = d.month 
   AND a.leg2dayofmonth = d.dayofmonth
   AND a.leg2dayofweek = d.`dayofweek` 
   AND d.origin = oa.iata 
   AND d.dest = da.iata 
   AND oa.country <> da.country ; 
```

Number of passengers on the airline that has long, planned layovers for an international
flight
```sql SELECT 
   a.leg1uniquecarrier as carrier, 
   count(a.leg1uniquecarrier) as passengers
FROM 
   unique_tickets a
where 
   a.leg2deptime - a.leg1arrtime>90
group by 
   a.leg1uniquecarrier;
```


Number of passengers on airlines that have elongated layovers for an international flight caused by delayed connection
```sql SELECT 
   a.leg1uniquecarrier as carrier, 
   count(a.leg1uniquecarrier) as passengers 
FROM 
   unique_tickets_orc a, 
   flights_orc o, 
   flights_orc d
where 
       a.leg1flightnum = o.flightnum
   AND a.leg1uniquecarrier = o.uniquecarrier 
   AND a.leg1origin = o.origin 
   AND a.leg1dest = o.dest 
   AND a.leg1month = o.month 
   AND a.leg1dayofmonth = o.dayofmonth
   AND a.leg1dayofweek = o.`dayofweek` 
   AND a.leg2flightnum = d.flightnum
   AND a.leg2uniquecarrier = d.uniquecarrier 
   AND a.leg2origin = d.origin 
   AND a.leg2dest = d.dest 
   AND a.leg2month = d.month 
   AND a.leg2dayofmonth = d.dayofmonth
   AND a.leg2dayofweek = d.`dayofweek` 
   AND o.depdelay > 60
group by 
   a.leg1uniquecarrier;
```

Number of passengers on airlines that have elongated layovers for an international flight caused by missed connection
```sql
SELECT 
   a.leg1uniquecarrier as carrier, 
   count(a.leg1uniquecarrier) as passengers
--   o.arrdelay as delay
FROM 
   unique_tickets_orc a, 
   flights_orc o, 
   flights_orc d
where 
       a.leg1flightnum = o.flightnum
   AND a.leg1uniquecarrier = o.uniquecarrier 
   AND a.leg1origin = o.origin 
   AND a.leg1dest = o.dest 
   AND a.leg1month = o.month 
   AND a.leg1dayofmonth = o.dayofmonth
   AND a.leg1dayofweek = o.`dayofweek` 
   AND a.leg2flightnum = d.flightnum
   AND a.leg2uniquecarrier = d.uniquecarrier 
   AND a.leg2origin = d.origin 
   AND a.leg2dest = d.dest 
   AND a.leg2month = d.month 
   AND a.leg2dayofmonth = d.dayofmonth
   AND a.leg2dayofweek = d.`dayofweek` 
   AND d.deptime-o.arrtime < o.arrdelay-45
group by 
   a.leg1uniquecarrier;
```
-----
## Lab 4 - Materialized View

*Do all these steps in the* **“db\_user001”..”db\_user020”** *unless otherwise noted.*

Create materialized view (MV). This will cause Hive to transparently rewrite queries, when possible, to use the MV instead of the base tables.

Add constraints for better query and refresh 
```sql
ALTER TABLE airlines_orc ADD CONSTRAINT airlines_pk PRIMARY KEY (code) DISABLE NOVALIDATE;
ALTER TABLE flights_orc ADD CONSTRAINT airlines_fk FOREIGN KEY (uniquecarrier) REFERENCES airlines_orc(code) DISABLE NOVALIDATE RELY;
```
Create Materialized View
```sql
DROP MATERIALIZED VIEW IF EXISTS traffic_cancel_airlines;
CREATE MATERIALIZED VIEW traffic_cancel_airlines
as SELECT airlines.code AS code,  MIN(airlines.description) AS description,
          flights.month AS month,
          sum(flights.cancelled) AS cancelled
FROM flights_orc flights JOIN airlines_orc airlines ON (flights.uniquecarrier = airlines.code)
group by airlines.code, flights.month;
```

Check that the Materialized view is created.
Replace ** in DB_USER0**
```sql
SHOW MATERIALIZED VIEWS;
```

Results

|MV_NAME | REWRITE_ENABLED |  MODE  |
| :- | :- | :- | 
|traffic_cancel_airlines|Yes	| Manual refresh |


Running a dashoboard query

```sql
SET hive.query.results.cache.enabled=false;

SELECT airlines.code AS code,  MIN(airlines.description) AS description, 
          sum(flights.cancelled) AS cancelled
FROM flights_orc flights , airlines_orc airlines 
WHERE flights.uniquecarrier = airlines.code
group by airlines.code;
```

Run the explain and query rewrite should show like:

![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.003.png)



Now you handling new data and refresh the materialized View


Create a temporary table for incremental data, insert 1000 rows with a new month and insert these into the partitioned by month fact table



```sql

drop table if exists flights_orc_incr;

create table flights_orc_incr
(dayofmonth int, dayofweek int, deptime int, crsdeptime int, arrtime int, 
 crsarrtime int, uniquecarrier string, flightnum int, tailnum string, 
 actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int, 
 depdelay int, origin string, dest string, distance int, taxiin int, 
 taxiout int, cancelled int, cancellationcode string, diverted string, 
 carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, 
 lateaircraftdelay int)
PARTITIONED BY (month int);


insert into flights_orc_incr select 13 as month, dayofmonth, dayofweek, deptime, crsdeptime, arrtime, crsarrtime, uniquecarrier, flightnum, tailnum, actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, origin, dest, distance, taxiin, taxiout, cancelled, cancellationcode, diverted, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay 
from flights_orc limit 1000;

INSERT into flights_orc select * from flights_orc_incr;

```

Update materialized view

```sql
ALTER MATERIALIZED VIEW traffic_cancel_airlines REBUILD;
```


Run dashboard query again to explore the usage of the MV. 

```sql
SET hive.query.results.cache.enabled=false;

SELECT airlines.code AS code,  MIN(airlines.description) AS description,
          flights.month AS month,
          sum(flights.cancelled) AS cancelled
FROM flights_orc flights , airlines_orc airlines 
WHERE flights.uniquecarrier = airlines.code
group by airlines.code, flights.month;
```

Disable materialized view rewrites
```sql
ALTER MATERIALIZED VIEW traffic_cancel_airlines DISABLE REWRITE;
```
Now repeat the first part of this step to see the different query plan, which no longer uses the MV.

Notice the difference in the explain 

With query rewrite read the **materialized view** : 

![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.003.png)

No query rewrite: Read flights (86mio rows) and airlines (1.5k rows) with merge join, group and sort

![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.004.png)

------
## Lab 5 - Slowly Changing Dimensions (SCD) - TYPE 2

*Do all these steps in the* **“db\_user001”..”db\_user020”** *unless otherwise noted.*

![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.005.png)

We create a new SDC table ***airline\_scd*** and add columns ***valid\_from*** and ***valid\_to***. Then loading the initial 1000 rows into this SDC table, then mock up new data and change data in the table ***airlines\_stage***. 

Finally merging these two tables with a single MERGE command to maintain the historical data and check the results.

Create the Hive managed table for airlines. Load initial by copy 1000 rows of current airlines with hard code the valid_from date

```sql
drop table if exists airlines_scd;

create table airlines_scd(code string, description string, valid_from date, valid_to date);

insert into airlines_scd 
  select *, cast('2021-01-01' as date), cast(null as date) 
  from airlines_csv limit 1000;
```

Create an external staging table pointing to our complete airlines dataset (1491 records) and update a description to mockup a change in the dimension

```sql
drop table if exists airlines_stage;

create table airlines_stage as select * from airlines_csv;

update airlines_stage set description =concat('Update - ',upper(description)) 
  where code in ('02Q','04Q');
```

Perform the SCD type 2 Merge Command

```sql
merge into airlines_scd 
using (
 -- The base staging data.
 select
   airlines_stage.code as join_key,
   airlines_stage.* from airlines_stage
 union all
 -- Generate an extra row for changed records.
 -- The null join_key means it will be inserted.
 select
   null, airlines_stage.*
 from
   airlines_stage join airlines_scd on airlines_stage.code = airlines_scd.code
 where
   ( airlines_stage.description <> airlines_scd.description )
   and airlines_scd.valid_to is null
) sub
on sub.join_key = airlines_scd.code
when matched
 and sub.description <> airlines_scd.description 
 then update set valid_to = current_date()
when not matched
 then insert values (sub.code, sub.description, current_date(), null);
```

View the changed records and see that the VALID_FROM and VALID_TO dates are set

```sql
select * from airlines_scd where code in ('02Q','04Q') order by code, valid_from;
```

Results


|CODE|DESCRIPTION|VALID\_FROM|VALID\_TO|
| :- | :- | :- | :- |
|02Q|Titan Airways|2021-01-01|2021-05-26|
|02Q|Update - TITAN AIRWAYS|2021-05-26|null|
|04Q|Tradewind Aviation|2021-01-01|2021-05-26|
|04Q|Update - TRADEWIND AVIATION|2021-05-26|null|



-----
## Lab 6 - Data Security & Governance 

The combination of the Data Warehouse with SDX offers a list of powerful features like rule-based masking columns based on a user’s role and/or group association or rule-based row filters. 

For this workshop we are going to explore Attribute-Based Access Control a.k.a. Tage-based security policies.

First we are going to create a series of tables in your work database. 

In the SQL editor, select your database and run this script:

```sql
CREATE TABLE emp_fname (id int, fname string);
insert into emp_fname(id, fname) values (1, 'Carl');
insert into emp_fname(id, fname) values (2, 'Clarence');

CREATE TABLE emp_lname (id int, lname string);
insert into emp_lname(id, lname) values (1, 'Rickenbacker');
insert into emp_lname(id, lname) values (2, 'Fender');

CREATE TABLE emp_age (id int, age smallint);
insert into emp_age(id, age) values (1, 35);
insert into emp_age(id, age) values (2, 55);

CREATE TABLE emp_denom (id int, denom char(2));
insert into emp_denom(id, denom) values (1, 'rk');
insert into emp_denom(id, denom) values (2, 'na');

CREATE TABLE emp_id (id int, empid integer);
insert into emp_id(id, empid) values (1, 1146651);
insert into emp_id(id, empid) values (2, 239125);

CREATE TABLE emp_all as
(select a.id, a.fname, b.lname, c.age, d.denom, e.empid from emp_fname a
	inner join emp_lname b on b.id = a.id
	inner join emp_age c on c.id = b.id
	inner join emp_denom d on d.id = c.id
	inner join emp_id e on e.id = d.id);

create table emp_younger as (select * from emp_all where emp_all.age <= 45);

create table emp_older as (select * from emp_all where emp_all.age > 45);
```

After this script executes, a simple

```sql
select * from emp_all;
```

… should give the contents of the emp\_all table, which only has a couple of lines of data.

For the next step we will switch to the UI of Atlas, the CDP component responsible for metadata management and governance: in the Cloudera Data Warehouse *Overview* UI, select your Virtual Warehouse to highlight the associated Database Catalog. Click on the three-dot menu of this DB catalog and select “Open Atlas” in the associated pop-up menu:

![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.006.png)

This should open the Atlas UI. CDP comes with a newer, improved user interface which can be enabled through the “Switch to Beta UI” link on the bottom right side of the screen. Do this now.

The Atlas UI has a left column which lists the Entities, Classifications, Business Metadata and Glossaries that belong to your CDP Environment.

![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.007.png)

We just created a couple of tables in the Data Warehouse, let’s look at the associated metadata. Under “Entities”, click on “hive\_db”. This should produce a list of databases.
Select you workshop database, this will result in the database’s metadata being displayed.

Select the “Tables” tab (the rightmost)
![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.008.png)

Select the “emp\_all” table from the list, this will result in Atlas displaying the metadata for this table; select the “lineage” tab:
   ![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.009.png)
This lineage graph shows the inputs, outputs as well as the processing steps resulting from the execution of our SQL code in the Data Warehouse. Clicking on one of the nodes will display a popup menu, which allows us to navigate through the lineage graph.
   Click on the “emp\_age” input table and select the link (the “guid” attribute) in the resulting popup menu:
   ![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.010.png)

In the screen that follows, select the “Schema” tab and in that table, click on the link for the “age” field:
   ![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.011.png)

[Explanation: we are now looking at the metadata page for the “age” column of the “emp\_age” table. There’s also a lineage tab here, because CDP tracks table- as well as column-based lineage for the Data Warehouse. What we want to do here: age is certainly a piece of sensitive personal information. We want to classify (‘tag’) it appropriately and then let SDX take care of treating this field as classified information that’s not visible to everyone.]

   Still in the screen for the “age” column, click on the plus sign next to “Classifications”; this will bring up a dialog:


   ![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.012.png)

   In the drop-down menu, select “PII” and make sure the “Propagate” checkbox is enabled.
   Click the “Add” button.
[This effectively means we apply the classification “PII” to the selected column and Atlas also will apply that classification to all columns that have been or will be derived from it.]

We can actually check this easily by using the lineage graph to navigate to a downstream table’s column: select one of the nodes that *don’t* have gear wheels (those are process information) and select the guid link.

This will give us the metadata for the “age” column in a derived table. Note the information on “Propagated Classifications”:
   ![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.013.png)
Try to query all columns from the “emp\_all” table again in DAS – by simply executing the last query again.
Why did we get an error now? There exists a policy in Ranger that denies all members of the hands-on lab group access to Hive data that is classified as “PII”. Let’s check that out. Like before for Atlas, open the Ranger UI via the triple-dot menu in you warehouse’s Database Catalog: ![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.014.png)

In the Ranger UI, select the “Audit” menu and limit the amount of data displayed by specifying the filter expressions:
   Result: Denied
   Service Type: HADOOP SQL

![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.015.png)


-----
## Lab 7 - Data Visualization


1. Use Data Visualization to further explore the data set.

`	`Open DataViz 


|**Step**|**Description**|
| :-: | :- |
|1|<p>Open Data Visualization ![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.016.png)</p><p></p><p></p><p>SSO login as user with password (not prompted) </p><p></p>|
|2|<p>Overview</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.017.png)</p>|
|3|<p>Switch to Data Tab</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.018.png)</p><p>Initially will have no Data Models</p>|
|4|<p>Upload Data - will create new table within the Database that you are connected to</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.019.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.020.png)</p><p>Don’t need to execute this steps, but is great to show for Self Service analytics (Data Scientists & Data Analy</p><p>sts</p>|
|5|<p>Build Data Model</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.021.png)</p><p>Create the Semantic Layer - data is not copied</p>|
|6|<p>Select Table or use SQL</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.022.png)</p><p></p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.023.png)</p><p></p><p></p><p>     </p><p>Ability to add as a table or enter your own pre-defined SQL</p>|
|7|<p>Edit Data Model</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.024.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.025.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.026.png)</p>|
|8|<p>Show Fields quickly</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.027.png)     ![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.028.png)</p><p></p><p>Can see that it created fields for each column in the table that was selected.</p><p></p>|
|9|<p>Join Planes table with Flights table</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.029.png)</p><p></p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.030.png)  </p><p></p><p></p><p>` `![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.031.png)</p>|
|10|<p>Join Airlines table with Flights table</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.032.png)  </p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.033.png)</p>|
|11|<p>Preview Data</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.034.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.035.png)</p><p></p><p>Scroll right</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.036.png)</p>|
|12|<p>Edit Fields</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.037.png)</p><p></p><p>Before</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.038.png)</p><p></p><p>You’ll use the following:</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.039.png)    ![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.040.png)</p><p></p><p>Edit Field properties</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.041.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.042.png)     ![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.043.png)</p><p></p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.044.png)</p><p></p><p>Create New Field</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.045.png) 1st clone</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.046.png)     ![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.047.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.048.png)</p><p></p><p>Change Display Name to “Route”</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.049.png)</p><p></p><p>Edit Expression</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.050.png)</p><p></p><p>Expression: </p><p>**concat( [origin],'-', [dest])**</p><p></p><p>Can Validate (to check for any errors) or Click Apply (to accept changes)</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.051.png) or ![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.052.png)</p>|
|13|<p>Finished Data Model</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.053.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.054.png)</p><p>Click Save</p>|
|14|<p>Create Dashboard</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.055.png)</p><p>` `![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.056.png)</p>|
|15|<p>First Visual</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.057.png)</p><p></p><p></p><p>Change Dep Delay Aggregate to Average</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.058.png)</p><p></p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.059.png)</p><p></p><p></p><p>Change to only show Top 25 Avgs</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.060.png)</p><p></p><p>Change Alias</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.061.png)</p><p></p><p>Finished</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.062.png)</p><p></p><p>Refresh Visual</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.063.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.064.png)</p><p></p><p>Add Title & Subtitle for Dashboard</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.065.png)</p><p></p><p>Add Title & Subtitle for this chart</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.066.png)</p><p></p>|
|16|<p>Second Visual</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.067.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.068.png)</p><p></p><p>Use Visual Styles to suggest charts</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.069.png)     ![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.070.png)</p><p></p><p>Select UniqueCarrier, Cancellationcode, Cancelled</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.071.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.072.png)</p><p></p><p>Filter for only cancelled flights</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.073.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.074.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.075.png)</p><p></p><p>Resize (make larger)</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.076.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.077.png)</p><p></p><p>Add Chart Title - “Cancellation Correlation”</p>|
|17|<p>Add Prompts</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.078.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.079.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.080.png)</p><p></p><p>Select values from prompt</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.081.png)</p><p></p><p></p><p></p>|
|18|<p>Third Visual (optional)</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.082.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.083.png)</p><p></p><p>Depending on user being used you may see the actual data not hashed (would need to login as Analyst user, and view this dashboard)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.084.png) </p><p></p><p>or </p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.085.png)</p><p></p><p>View Tooltips (click Dep Delay value)</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.086.png)</p><p>Use to show the Ranger security policy taking effect</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.087.png)</p>|
|19|<p>Save Dashboard</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.088.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.089.png)</p>|
|20|<p>View Dashboard</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.090.png) click on visuals tab </p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.091.png)</p><p></p><p>Click on Dashboard</p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.092.png)</p><p></p><p>![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.093.png)</p>|

||
| :- |


-----
## Bonus Material (optional)
Run load test to simulate adding many more end users. Then view results of autoscaling. Discuss how autoscaling works, and how it allows for easy, cost-effective scaling. 

|-- Optional step. Can be just a discussion if no load test is actually done.|
| :- |

Discuss how workload aware autoscaling runs ETL style queries on dedicated executor group to not interfere with shorter queries. 

|<p>-- Start the creation process for a Hive VW and see the **“Query Isolation” option.**</p><p>-- No need to continue to create the VW.</p>|
| :- |

Clean Up 
```sql
DROP DATABASE DBB_USER0** CASCADE;
```

### HPLSQL 


Login into a K8s pod with hiveserver2 CDW and create the procedures


This HPLSQL Package run a analyse by airport and list the top delayed flights inone single field (denormalized).

SQL Procedures Script - copy and save in aa file: vhol.hql 

```sql

use airlinedata;

create or replace package airport_experience AS
 MAX_FLIGHTS int := 3;
 procedure describe();
 procedure dbg (debug_level integer, msg string);
 procedure total_arrival_delay ( IN v_iata string, OUT v_top_flights string, OUT v_totaldelay double);
 procedure generate( v_airports varchar default 'JFK');
END;

create or replace package body airport_experience AS

procedure describe () is
begin
 dbms_output.put_line('Package airport_experiences');
 dbms_output.put_line('Version: 0.0.1');
 dbms_output.put_line('Collection of prodecures to calcluate airpoort experience')
 dbms_output.put_line('prodecures : generate () no parameter')
 dbms_output.put_line('result set : table : airport_experiences')
end;

procedure dbg (debug_level integer, msg string)
is
BEGIN
declare ts string default SYSDATE;
declare lvl string default 'INFO';
if debug_level > 0 
 Begin

 if debug_level > 10 SET lvl := 'ERROR';

  DBMS_OUTPUT.PUT_LINE( ts || ':' ||lvl|| ': ' || msg); 
 end;
EXCEPTION WHEN OTHERS THEN
  dbg(99,'Error: procedure dbg');
end;

procedure total_arrival_delay ( IN v_iata string, OUT v_top_flights string, OUT v_totaldelay double)
is
BEGIN
  declare debug_level integer default 1;
  declare v_flight string default  '';
  declare v_sum_delay double;
  declare i int default  0; 
  declare v_d double;

  DECLARE cur CURSOR FOR SELECT concat(uniquecarrier, flightnum) as flight_num, sum(arrdelay) as sum_delay
        from flights_orc
        where origin = v_iata 
        group by concat(uniquecarrier,flightnum)
	having sum(arrdelay) is not null
        order by sum_delay DESC;

  dbg(debug_level, 'pro: total_arriaval_delay v_iata value: ' || v_iata || ' v_arrdelay: '||v_arr_delay );
  v_top_flights = '';

  select sum(arrdelay) into v_totaldelay
    from flights_orc 
    where origin = v_iata; 

  dbg(debug_level, 'pro: fetch total delay value: ' || v_totaldelay );
 

  if v_totaldelay <> 0  then
    begin
    dbg(debug_level, 'pro: delays found');

    OPEN cur;

    dbg(debug_level,'pro: cursor open');

    FETCH cur INTO  v_flight, v_sum_delay;
     WHILE SQLCODE=0 and i < MAX_FLIGHTS THEN
      set i := i + 1;

      dbg(debug_level,'pro: fetched to ' || i || ' flight : ' || v_flight );
 
      SET v_top_flights = v_top_flights || v_flight ||':'||v_sum_delay||';'
    FETCH cur INTO  v_flight, v_sum_delay;
   END WHILE;
   CLOSE cur;

   end;
   else
    begin
    dbg(debug_level,'pro: no delays found ');
     v_totaldelay = 0;
    end;
 end if;

dbg( debug_level,'end: top_flights: '||v_top_flights);

EXCEPTION WHEN OTHERS THEN
  dbg(99,'OTHERS: total_arrival_delay()');
END;

procedure generate( v_airports varchar default 'JFK')
IS
BEGIN
DECLARE debug_level integer default 1;
DECLARE v_iata string default 'JFK';
DECLARE v_top string default '#';
DECLARE v_total double default 0;
DECLARE v_c char;
DECLARE ts timestamp;
DECLARE v_msg string default '';

dbg(debug_level, 'main: 1 airports ' || v_airports);
select "'" into v_c;

DECLARE cur CURSOR FOR 'SELECT iata from airports_orc where iata in ( ' || v_c || v_airports || v_c || ')';

dbg(debug_level, 'main: 1');

OPEN cur;

  dbg(debug_level,'main: 2 - cursor open');

FETCH cur INTO v_iata; 
WHILE SQLCODE=0 THEN
  v_top = '#';
  v_total = 0; 

  dbg(debug_level,'main: 3 fetch - iata: ' || v_iata) ;
  call total_arrival_delay(v_iata,v_top,v_total );
  dbg(debug_level,  'main: 4 called proc total_arrival_delay ');

  v_msg = 'airport:'||v_iata|| ' top flights: '|| v_top ||' total delay:'||v_total;
  dbg(debug_level,  'main: 4 IN_OUT '||v_msg);

  insert into  airports_experiences values( v_iata, v_top, v_total);

  FETCH cur INTO v_iata; 
END WHILE;
CLOSE cur;

  dbg(debug_level ,'main: 5 - finished');

EXCEPTION WHEN OTHERS THEN
  dbg(99,'Error: main');
END;

end;


drop table if exists airports_experiences;
create table airports_experiences(iata string, delay_top_flights string, delay_total double  ) ;
begin
 CALL airport_experience.describe();
 CALL airport_experience.generate( 'SFO');
 CALL airport_experience.generate( 'JFK');
 CALL airport_experience.generate( 'BOS');
 CALL airport_experience.generate( '*');
end;
select * from airports_experiences;

```

Run beeline

```sql
$ beeline -u "jdbc:hive2://hs2-vhol-cdw2-nosso.env-hvmrdx.dw.a465-9q4k.cloudera.site/default;transportMode=http;httpPath=cliservice;ssl=true;retries=3;mode=hplsql;" -n frothkoetter -p $PW -f /tmp/vhol.hql
```

Results

| airports_stats.iata  |        airports_stats.delay_top_flights        | airports_stats.delay_total  |
| :- | :- | :- |
| JFK                  | AA647:91067.0;AA177:87305.0;AA1639:82770.0;    | 1.0155716E7                 |
| LAX                  | DL1579:68519.0;DL1565:49367.0;WN1517:48037.0;  | 1.795024E7                  |

SQL to break the field into multi rows

```sql
select iata, cast(delay_total as bigint) 
, split(flights,"\\:")[0] as flug
, split(flights,"\\:")[1] as delay
, (100 / delay_total * cast (split(flights,"\\:")[1] as integer)) as pct
   from ( select iata,
	          delay_total,
	          split(delay_top_flights,"\\|")  top_flights
	    from airports_stats ) f
		lateral VIEW explode(f.top_flights) bar AS flights
```

### Iceberg - Timetravel

Create a partitioned table
The CREATE TABLE ... PARTITIONED BY syntax enables you to create identity-partitioned Iceberg tables. Identity-partitioned Iceberg tables are similar to the regular partitioned tables and are stored in the same directory structure as the regular partitioned tables. The difference is that the data files in the identity-partitioned Iceberg tables continue to store the partitioning columns. 


Only identity-partitioned Iceberg tables can be created through Hive. If other systems create Iceberg tables with different partitioning, then they could still be read from Hive.


```sql
set iceberg.mr.catalog = hive;
set hive.vectorized.execution.enabled = false;
set hive.tez.mapreduce.output.committer.on.hs2=true; 

drop table if exists flights_ice;

CREATE EXTERNAL TABLE flights_ice(dayofmonth int, 
 dayofweek int, deptime int, crsdeptime int, arrtime int, 
 crsarrtime int, uniquecarrier string, flightnum int, tailnum string, 
 actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int, 
 depdelay int, origin string, dest string, distance int, taxiin int, 
 taxiout int, cancelled int, cancellationcode string, diverted string, 
 carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, 
lateaircraftdelay int) 
partitioned by (month int) 
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler';

insert into flights_ice 
select month, dayofmonth, dayofweek, deptime, crsdeptime, arrtime, crsarrtime, uniquecarrier, flightnum, tailnum, actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, origin, dest, distance, taxiin, taxiout, cancelled, cancellationcode, diverted, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay 
from flights_csv; 
```


### Data Sketches

Create a table for data sketch columns
```sql
drop table if exists airlinedata.flights_airlines_sk;

create table airlinedata.flights_airlines_sk as
select flights_orc_partitioned.uniquecarrier AS airline_code,
 count(1) as sum_flights,
 sketch.qt_data2sketch(cast(arrdelay as double)) as sk_arrdelay
FROM airlinedata.flights_orc
where arrdelay > 0
GROUP BY uniquecarrier;
							
```
Fast retrieval a few rows 
```sql
select airline_code,sum_flights,sum_arrdelay,
	  sketch.qt_getPmf(sk_arrdelay,10,20,30,40,50,60,70,80,90,100),			
	  sketch.qt_getCdf(sk_arrdelay,10,20,30,40,50,60,70,80,90,100),
	  sketch.qt_getK(sk_arrdelay),						
	  sketch.qt_getN(sk_arrdelay)	
from airlinedata.flights_airlines_sk
group by airline_code
```

![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.094.png)

```sql
drop view if exists airlinedata.vw_flights_sk_airlines;
create view airlinedata.vw_flights_sk_airlines as					
select airline_code, 
   sum_flights, sum_arrdelay, min_arrdelay, max_arrdelay, avg_arrdelay,
   sketch.qt_getQuantile(sk_arrdelay,0.1) as arrival_delay_10,
   sketch.qt_getQuantile(sk_arrdelay,0.2) as arrival_delay_20,
   sketch.qt_getQuantile(sk_arrdelay,0.3) as arrival_delay_30,
   sketch.qt_getQuantile(sk_arrdelay,0.4) as arrival_delay_40,
   sketch.qt_getQuantile(sk_arrdelay,0.5) as arrival_delay_50,
   sketch.qt_getQuantile(sk_arrdelay,0.6) as arrival_delay_60,
   sketch.qt_getQuantile(sk_arrdelay,0.7) as arrival_delay_70,
   sketch.qt_getQuantile(sk_arrdelay,0.8) as arrival_delay_80,
   sketch.qt_getQuantile(sk_arrdelay,0.9) as arrival_delay_90,
   sketch.qt_getQuantile(sk_arrdelay,1.0) as arrival_delay_100
from airlinedata.flights_airlines_sk;

select * from airlinedata.vw_flights_sk_airlines;
```

PDF / CDF - Airline Histogram -  create Distribution Histograms (here as Excel) 

![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.095.png)


Count distinct with HLL algorithm  


Optional step how many unique flights 

```sql
create table airlinedata.flights_sketch as
select sketch.hll_data2sketch( cast(concat(flights_orc.uniquecarrier,flights_orc.flightnum) as string) ) AS flightnum_sk
FROM airlinedata.flights_orc;

select sketch.hll_estimate(flightnum_sk)
from airlinedata.flights_sketch;
```

<p>Results</p><p>     </p>|


|44834.13712876354|
| :- |



Explain - extreme fast query a table 

![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.096.png)

alternative classic query would be

```sql
select count(distinct(cast(concat(flights\_orc.uniquecarrier,flights\_orc.flightnum) as string))) from airlinedata.flights_orc;
```
</p><p>Results</p><p>     </p>|


|44684|
| :- |



Explain - query full fact table with going over 86mio of the fact table

![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.097.png)

![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.098.png)

#### Needle in Haystack - most frequency items - or better not on Alaska Airline Flight - AS65

Optional step. Can be just a discussion

What flights are most frequently cancelled

```sql
drop table if exists airlinedata.flights_frq_sketch; 					  
create table airlinedata.flights_frq_sketch (cancelled int, sk_flightnum binary);
insert into airlinedata.flights_frq_sketch 
select flights_orc.cancelled, sketch.frq_data2sketch( cast(concat(flights_orc.uniquecarrier,flights_orc.flightnum) as string), 8192 )
FROM airlinedata.flights_orc
GROUP BY flights_orc.cancelled;
							
select sketch.frq_get_items(sk_flightnum, 'NO_FALSE_POSITIVES')
from airlinedata.flights_frq_sketch;
```

<p>Results</p><p>     </p>|


|ITEM|ESTIMATE|LOWER\_BOUND|UPPER\_BOUND|
| :- | :- | :- | :- |
|AS65|957|586|957|
|WN25|929|558|929|
|AS64|884|513|884|




validate the results

```sql
select concat(flights_orc.uniquecarrier,flights_orc.flightnum) as flight, 
       count(1) as num_cancelled
from airlinedata.flights_orc 
where flightnum = 65 and cancelled = 1
group by concat(flights_orc.uniquecarrier,flights_orc.flightnum)
order by num_cancelled desc;
```

Results

|FLIGHT|NUM\_CANCELLED|
| :- | :- |
|AS65|940|
|TW65|111|
|US65|74|




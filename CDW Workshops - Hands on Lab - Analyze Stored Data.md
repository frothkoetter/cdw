

# CDW Workshops - vHoL  

Analyze Stored Data

## Introduction
This is a high level overview of how to use the Cloudera Data Warehouse service to quickly explore raw data, create curated versions of the data for simple reporting and dashboarding, and then scale up usage of the curated data by exposing it to more users. It highlights the performance and automation capabilities that help ensure performance is maintained while controlling cost.  

ER - Diagram of the demo: fact table flights (86mio rows) and dimension tables: airlines (1.5k rows), airports (3.3k rows) and planes (5k rows)

![](images/Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.002.png)

## Lab Setup


1. Ensure that you have set your workload password.


|<p>Click on username in the bottom left, then Profile, then “Set Workload Password” link.</p><p>Then enter and confirm the password. Then “**Set Workload Password”**.</p>|
| :- |



## Lab 1 - Create Database
*Do all these steps as the* **“db\_user001”..”db\_user020”** *unless otherwise noted.*

1. Create new database to be used or use one that is already created for you

```sql
-- Change *** of database name
CREATE DATABASE DB_USER0**;
USE DB_USER0***;

```
| :- |

-----
## Lab 2 - External Tables

1. Run DDL to create external tables on the CSV data files, which are already in cloud object storage.

|<p></p><p>drop table if exists flights\_csv;</p><p>CREATE EXTERNAL TABLE flights\_csv(month int, dayofmonth int, </p><p>` `dayofweek int, deptime int, crsdeptime int, arrtime int, </p><p>` `crsarrtime int, uniquecarrier string, flightnum int, tailnum string, </p><p>` `actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int, </p><p>` `depdelay int, origin string, dest string, distance int, taxiin int, </p><p>` `taxiout int, cancelled int, cancellationcode string, diverted string, </p><p>` `carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, </p><p>lateaircraftdelay int) </p><p>ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' </p><p>STORED AS TEXTFILE LOCATION '/airlinedata-csv/flights' tblproperties("skip.header.line.count"="1");</p><p></p><p>drop table if exists planes\_csv;</p><p>CREATE EXTERNAL TABLE planes\_csv(tailnum string, owner\_type string, manufacturer string, issue\_date string, model string, status string, aircraft\_type string, engine\_type string, year int) </p><p>ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' </p><p>STORED AS TEXTFILE LOCATION '/airlinedata-csv/planes' tblproperties("skip.header.line.count"="1");</p><p></p><p>drop table if exists airlines\_csv;</p><p>CREATE EXTERNAL TABLE airlines\_csv(code string, description string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' </p><p>STORED AS TEXTFILE LOCATION '/airlinedata-csv/airlines' tblproperties("skip.header.line.count"="1");</p><p></p><p>drop table if exists airports\_csv;</p><p>CREATE EXTERNAL TABLE airports\_csv(iata string, airport string, city string, state DOUBLE, country string, lat DOUBLE, lon DOUBLE) </p><p>ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' </p><p>STORED AS TEXTFILE LOCATION '/airlinedata-csv/airports' tblproperties("skip.header.line.count"="1");</p>|
| :- |

1. Check that you created tables

|<p>**use DB\_USER001;**</p><p></p><p>show tables;</p><p></p>|
| :- |


|<p>**Results**</p><p></p><p>TAB\_NAME</p><p>airlines\_csv</p><p>airports\_csv</p><p>flights\_csv</p><p>planes\_csv</p><p></p>|
| :- |


1. Run exploratory queries to understand the data. This reads the CSV data, converts it into a columnar in-memory format, and executes the query.

|<p>-- NAME: Airline Delay Aggregate Metrics by Airplane</p><p>-- DESCRIPTION: Customer Experience Reporting - Report showing airplanes that have the</p><p>-- highest average delays, causing the worst customer experience.</p><p></p><p>**USE DB\_USER001;**</p><p></p><p>SELECT tailnum,</p><p>`       `count(\*),</p><p>`       `avg(depdelay) AS avg\_delay,</p><p>`       `max(depdelay),</p><p>`       `avg(taxiout),</p><p>`       `avg(cancelled),</p><p>`       `avg(weatherdelay),</p><p>`       `max(weatherdelay),</p><p>`       `avg(nasdelay),</p><p>`       `max(nasdelay),</p><p>`       `avg(securitydelay),</p><p>`       `max(securitydelay),</p><p>`       `avg(lateaircraftdelay),</p><p>`       `max(lateaircraftdelay),</p><p>`       `avg(airtime),</p><p>`       `avg(actualelapsedtime),</p><p>`       `avg(distance)</p><p>FROM flights\_csv</p><p>GROUP BY tailnum</p><p>ORDER BY avg\_delay DESC;</p><p></p><p></p><p>-- NAME: Engine Types Causing Most Delays</p><p>-- DESCRIPTION: Ad Hoc Exploration to Investigate - Exploratory query to </p><p>-- determine which engine type contributes to the most delayed flights.</p><p>-- If this returns no results, then remove the 'WHERE tailnum in …' clause</p><p></p><p></p><p>SELECT model,</p><p>`       `engine\_type</p><p>FROM planes\_csv</p><p>WHERE planes\_csv.tailnum IN</p><p>`    `(SELECT tailnum</p><p>`     `FROM</p><p>`       `(SELECT tailnum,</p><p>`               `count(\*),</p><p>`               `avg(depdelay) AS avg\_delay,</p><p>`               `max(depdelay),</p><p>`               `avg(taxiout),</p><p>`               `avg(cancelled),</p><p>`               `avg(weatherdelay),</p><p>`               `max(weatherdelay),</p><p>`               `avg(nasdelay),</p><p>`               `max(nasdelay),</p><p>`               `avg(securitydelay),</p><p>`               `max(securitydelay),</p><p>`               `avg(lateaircraftdelay),</p><p>`               `max(lateaircraftdelay),</p><p>`               `avg(airtime),</p><p>`               `avg(actualelapsedtime),</p><p>`               `avg(distance)</p><p>`        `FROM flights\_csv</p><p>`        `WHERE tailnum IN ('N194JB',</p><p>`                          `'N906S',</p><p>`                          `'N575ML',</p><p>`                          `'N852NW',</p><p>`                          `'N000AA')</p><p>`        `GROUP BY tailnum) AS delays);</p>|
| :- |



-----
## Lab 3 - Managed Tables

1. Run “CREATE TABLE AS SELECT” queries to create full ACID ORC type of the tables. This creates curated versions of the data which are optimal for BI usage.

*Do all these steps in the* **“db\_user001”..”db\_user020”** *unless otherwise noted.*


|<p>**USE DB\_USER001**</p><p></p><p>drop table if exists airlines\_orc;</p><p>create table airlines\_orc as select \* from airlines\_csv;</p><p></p><p>drop table if exists airports\_orc;</p><p>create table airports\_orc as select \* from airports\_csv;</p><p></p><p>drop table if exists planes\_orc;</p><p>create table planes\_orc as select \* from planes\_csv;</p><p></p><p>drop table if exists flights\_orc;</p><p>create table flights\_orc partitioned by (month) as </p><p>select month, dayofmonth, dayofweek, deptime, crsdeptime, arrtime, crsarrtime, uniquecarrier, flightnum, tailnum, actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, origin, dest, distance, taxiin, taxiout, cancelled, cancellationcode, diverted, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay </p><p>from flights\_csv;</p><p></p><p></p><p></p>|
| :- |

1. Check that you created managed & external tables

|<p>use **DB\_USER001;**</p><p></p><p>show tables;</p>|
| :- |


|<p>Results</p><p></p><p>TAB\_NAME</p><p>airlines\_csv</p><p>airlines\_orc</p><p>airports\_csv</p><p>airports\_orc</p><p>flights\_csv</p><p>flights\_orc</p><p>planes\_csv</p><p>planes\_orc</p>|
| :- |



1. Experiment with different queries to see effects of the data cache on each executor.

|<p>-- Run query. Highlight both “SET …” and “SELECT …” when you execute.</p><p>SET hive.query.results.cache.enabled=false;</p><p></p><p></p><p>SELECT</p><p>`  `SUM(flights.cancelled) AS num\_flights\_cancelled,</p><p>`  `SUM(1) AS total\_num\_flights,</p><p>`  `MIN(airlines.description) AS airline\_name,</p><p>`  `airlines.code AS airline\_code</p><p>FROM</p><p>`  `flights\_orc flights</p><p>`  `JOIN airlines\_orc airlines ON (flights.uniquecarrier = airlines.code)</p><p>GROUP BY</p><p>`  `airlines.code</p><p>ORDER BY</p><p>`  `num\_flights\_cancelled DESC;</p><p></p><p>-- Go to Queries page, then click on the query that just ran, then scroll down to DAG INFO,</p><p>-- then choose DAG COUNTERS, then filter for 'cache', then show CACHE\_MISS\_BYTES and/or </p><p>-- CACHE\_HIT\_BYTES. Take note of the query run time too.</p><p></p><p>-- Note that sometimes it takes up to a few minutes for DAS to parse the </p><p>-- query metrics and expose them in the UI.</p><p></p><p>-- Run query again</p><p>-- then check the cache metrics again to see the improved hit rate.</p>|
| :- |




-----
## Lab 4 - Materialized View

1. Create materialized view (MV). This will cause Hive to transparently rewrite queries, when possible, to use the MV instead of the base tables.


|<p>USE **DB\_USER001**;</p><p></p><p>-- add required constraints</p><p></p><p>ALTER TABLE airlines\_orc ADD CONSTRAINT airlines\_pk PRIMARY KEY (code) DISABLE NOVALIDATE;</p><p>ALTER TABLE flights\_orc ADD CONSTRAINT airlines\_fk FOREIGN KEY (uniquecarrier) REFERENCES airlines\_orc(code) DISABLE NOVALIDATE RELY;</p><p></p><p>-- create MV</p><p></p><p>DROP MATERIALIZED VIEW IF EXISTS traffic\_cancel\_airlines</p><p>CREATE MATERIALIZED VIEW traffic\_cancel\_airlines</p><p>as SELECT airlines.code AS code,  MIN(airlines.description) AS description,</p><p>`          `flights.month AS month,</p><p>`          `sum(flights.cancelled) AS cancelled,</p><p>`          `count(flights.diverted) AS diverted</p><p>FROM flights\_orc flights JOIN airlines\_orc airlines ON (flights.uniquecarrier = airlines.code)</p><p>group by airlines.code, flights.month;</p><p></p><p>-- show MV</p><p>SHOW MATERIALIZED VIEWS in **DB\_USER001;**</p><p></p>|
| :- |

1. Incremental refresh the materialized View


|<p>-- generate 1000 rows as new arrival data </p><p>USE DB\_USER001;</p><p></p><p>drop table if exists flights\_orc\_incr;</p><p>create table flights\_orc\_incr</p><p>(dayofmonth int, dayofweek int, deptime int, crsdeptime int, arrtime int, </p><p>` `crsarrtime int, uniquecarrier string, flightnum int, tailnum string, </p><p>` `actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int, </p><p>` `depdelay int, origin string, dest string, distance int, taxiin int, </p><p>` `taxiout int, cancelled int, cancellationcode string, diverted string, </p><p>` `carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, </p><p>` `lateaircraftdelay int)</p><p>PARTITIONED BY (month int);</p><p></p><p>use DB\_USER001;</p><p>insert into flights\_orc\_incr select 15 as month, dayofmonth, dayofweek, deptime, crsdeptime, arrtime, crsarrtime, uniquecarrier, flightnum, tailnum, actualelapsedtime, crselapsedtime, airtime, arrdelay, depdelay, origin, dest, distance, taxiin, taxiout, cancelled, cancellationcode, diverted, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay </p><p>from flights\_orc limit 1000;</p><p></p><p>USE DB\_USER001;</p><p>Insert into flights\_orc select \* from flights\_orc\_incr;</p><p></p><p>-- update materialized view</p><p>USE DB\_USER001;</p><p>ALTER MATERIALIZED VIEW traffic\_cancel\_airlines REBUILD;</p><p></p><p></p>|
| :- |


1. Run dashboard query again to explore the usage of the MV. 


|<p></p><p>-- query cancelled flights by airline</p><p></p><p>USE DB\_USER001;</p><p>SET hive.query.results.cache.enabled=false;</p><p>SELECT airlines.code AS code,  MIN(airlines.description) AS description,</p><p>`          `flights.month AS month,</p><p>`          `sum(flights.cancelled) AS cancelled</p><p>FROM flights\_orc flights , airlines\_orc airlines </p><p>WHERE flights.uniquecarrier = airlines.code</p><p>group by airlines.code, flights.month;</p><p></p><p>-- Disable materialized view rewrites</p><p>use DB\_USER001;</p><p>ALTER MATERIALIZED VIEW traffic\_cancel\_airlines DISABLE REWRITE;</p><p></p><p>-- Now repeat the first part of this step to see the different query plan, </p><p>-- which no longer uses the MV.</p>|
| :- |

Notice the difference in the explain 

With query rewrite read the **materialized view** : 

`   `![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.003.png)

No query rewrite: Read flights (86mio rows) and airlines (1.5k rows) with merge join, group and sort

![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.004.png)
## -----Lab 5 - Slowly Changing Dimensions (SCD) - TYPE 2

![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.005.png)

1. We create a new SDC table ***airline\_scd*** table and add columns valid\_from and valid\_to. Then loading the initial 1000 rows into this SDC table. 

Next step is to mock up new data and change data in the table ***airlines\_stage***. 

Finally merging these two tables with a single MERGE command to maintain the historical data and check the results.


|<p>use **db\_user001;**</p><p></p><p>-- Create the Hive managed table for our contacts. We track a start and end date.</p><p>drop table if exists airlines\_scd;</p><p>create table airlines\_scd(code string, description string, valid\_from date, valid\_to date);</p><p></p><p></p><p>-- Load initial by copy 1000 rows of current airlines table into the airlimanaged table, We hard code the valid\_from dates to the beginning of 2021</p><p>insert into airlines\_scd select \*, cast('2021-01-01' as date), cast(null as date) from airlines\_csv limit 1000;</p><p></p><p>-- Create an external table pointing to our complete airlines dataset (1491 records)</p><p>Drop table if exists airlines\_stage;</p><p>create table airlines\_stage as select \* from airlines\_csv;</p><p></p><p>-- Update a description to mockup a change in the dimension</p><p>update airlines\_stage set description ='SDC Demo Update' where code in ('02Q','04Q')</p><p></p><p></p><p>-- Perform the SCD type 2 </p><p>merge into airlines\_scd </p><p>using (</p><p>` `-- The base staging data.</p><p>` `select</p><p>`   `airlines\_stage.code as join\_key,</p><p>`   `airlines\_stage.\* from airlines\_stage</p><p>` `union all</p><p>` `-- Generate an extra row for changed records.</p><p>` `-- The null join\_key means it will be inserted.</p><p>` `select</p><p>`   `null, airlines\_stage.\*</p><p>` `from</p><p>`   `airlines\_stage join airlines\_scd on airlines\_stage.code = airlines\_scd.code</p><p>` `where</p><p>`   `( airlines\_stage.description <> airlines\_scd.description )</p><p>`   `and airlines\_scd.valid\_to is null</p><p>) sub</p><p>on sub.join\_key = airlines\_scd.code</p><p>when matched</p><p>` `and sub.description <> airlines\_scd.description </p><p>` `then update set valid\_to = current\_date()</p><p>when not matched</p><p>` `then insert values (sub.code, sub.description, current\_date(), null);</p><p></p><p></p><p>-- Confirm we now have 1493 records.</p><p>select count(\*) from airlines\_scd;</p><p></p><p></p><p>-- View the changed records </p><p>select \* from airlines\_scd where code in ('02Q','04Q')</p><p></p>|
| :- |


Results



|AIRLINES\_SCD.CODE|AIRLINES\_SCD.DESCRIPTION|AIRLINES\_SCD.VALID\_FROM|AIRLINES\_SCD.VALID\_TO|
| :- | :- | :- | :- |
|02Q|Titan Airways|2021-01-01|2021-05-26|
|04Q|Tradewind Aviation|2021-01-01|2021-05-26|
|02Q|SDC Demo Update|2021-05-26|null|
|04Q|SDC Demo Update|2021-05-26|null|




-----
## Lab - Data Security & Governance 

1. The combination of the Data Warehouse with SDX offers a list of powerful features like rule-based masking columns based on a user’s role and/or group association or rule-based row filters. 
   For this workshop we are going to explore Attribute-Based Access Control a.k.a. Tage-based security policies.
1. First we are going to create a series of tables in your work database. 
   In the SQL editor, select your database and run this script:

|<p>-- In DAS, run the following query</p><p></p><p>CREATE TABLE emp\_fname (id int, fname string);</p><p>insert into emp\_fname(id, fname) values (1, 'Carl');</p><p>insert into emp\_fname(id, fname) values (2, 'Clarence');</p><p></p><p>CREATE TABLE emp\_lname (id int, lname string);</p><p>insert into emp\_lname(id, lname) values (1, 'Rickenbacker');</p><p>insert into emp\_lname(id, lname) values (2, 'Fender');</p><p></p><p>CREATE TABLE emp\_age (id int, age smallint);</p><p>insert into emp\_age(id, age) values (1, 35);</p><p>insert into emp\_age(id, age) values (2, 55);</p><p></p><p>CREATE TABLE emp\_denom (id int, denom char(2));</p><p>insert into emp\_denom(id, denom) values (1, 'rk');</p><p>insert into emp\_denom(id, denom) values (2, 'na');</p><p></p><p>CREATE TABLE emp\_id (id int, empid integer);</p><p>insert into emp\_id(id, empid) values (1, 1146651);</p><p>insert into emp\_id(id, empid) values (2, 239125);</p><p></p><p>CREATE TABLE emp\_all as</p><p>(select a.id, a.fname, b.lname, c.age, d.denom, e.empid from emp\_fname a</p><p>`	`inner join emp\_lname b on b.id = a.id</p><p>`	`inner join emp\_age c on c.id = b.id</p><p>`	`inner join emp\_denom d on d.id = c.id</p><p>`	`inner join emp\_id e on e.id = d.id);</p><p></p><p>create table emp\_younger as (select \* from emp\_all where emp\_all.age <= 45);</p><p></p><p>create table emp\_older as (select \* from emp\_all where emp\_all.age > 45);</p>|
| :- |

1. After this script executes, a simple

|<p>-- In DAS, run the following query</p><p></p><p>select \* from emp\_all;</p>|
| :- |
… should give the contents of the emp\_all table, which only has a couple of lines of data.

1. For the next step we will switch to the UI of Atlas, the CDP component responsible for metadata management and governance: in the Cloudera Data Warehouse *Overview* UI, select your Virtual Warehouse to highlight the associated Database Catalog. Click on the three-dot menu of this DB catalog and select “Open Atlas” in the associated pop-up menu:

![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.006.png)

1. This should open the Atlas UI. CDP comes with a newer, improved user interface which can be enabled through the “Switch to Beta UI” link on the bottom right side of the screen. Do this now.
   The Atlas UI has a left column which lists the Entities, Classifications, Business Metadata and Glossaries that belong to your CDP Environment.
   ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.007.png)
1. We just created a couple of tables in the Data Warehouse, let’s look at the associated metadata. Under “Entities”, click on “hive\_db”. This should produce a list of databases.
1. Select you workshop database, this will result in the database’s metadata being displayed.
1. Select the “Tables” tab (the rightmost)
   ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.008.png)
1. Select the “emp\_all” table from the list, this will result in Atlas displaying the metadata for this table; select the “lineage” tab:
   ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.009.png)
1. This lineage graph shows the inputs, outputs as well as the processing steps resulting from the execution of our SQL code in the Data Warehouse. Clicking on one of the nodes will display a popup menu, which allows us to navigate through the lineage graph.
   Click on the “emp\_age” input table and select the link (the “guid” attribute) in the resulting popup menu:
   ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.010.png)
1. In the screen that follows, select the “Schema” tab and in that table, click on the link for the “age” field:
   ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.011.png)
1. [Explanation: we are now looking at the metadata page for the “age” column of the “emp\_age” table. There’s also a lineage tab here, because CDP tracks table- as well as column-based lineage for the Data Warehouse. 
   What we want to do here: age is certainly a piece of sensitive personal information. We want to classify (‘tag’) it appropriately and then let SDX take care of treating this field as classified information that’s not visible to everyone.]
   Still in the screen for the “age” column, click on the plus sign next to “Classifications”; this will bring up a dialog:
   ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.012.png)
   In the drop-down menu, select “PII” and make sure the “Propagate” checkbox is enabled.
   Click the “Add” button.
1. [This effectively means we apply the classification “PII” to the selected column and Atlas also will apply that classification to all columns that have been or will be derived from it.]
   We can actually check this easily by using the lineage graph to navigate to a downstream table’s column: select one of the nodes that *don’t* have gear wheels (those are process information) and select the guid link.
1. This will give us the metadata for the “age” column in a derived table. Note the information on “Propagated Classifications”:
   ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.013.png)
1. Try to query all columns from the “emp\_all” table again in DAS – by simply executing the last query again.
1. Why did we get an error now? There exists a policy in Ranger that denies all members of the hands-on lab group access to Hive data that is classified as “PII”. Let’s check that out. Like before for Atlas, open the Ranger UI via the triple-dot menu in you warehouse’s Database Catalog: ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.014.png)
1. In the Ranger UI, select the “Audit” menu and limit the amount of data displayed by specifying the filter expressions:
   Result: Denied
   Service Type: HADOOP SQL

![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.015.png)

1. [tbd. Test the group mechanism, explain more]



-----
## Lab - Data Visualization


1. Use Data Visualization to further explore the data set.

`	`Open DataViz 



|**Step**|**Description**|||
| :-: | :- | :- | :- |
|1|<p>Open Data Visualization ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.016.png)</p><p></p><p></p><p>SSO login as user with password (not prompted) </p><p></p>|
|2|<p>Overview</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.017.png)</p>|
|3|<p>Switch to Data Tab</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.018.png)</p><p>Initially will have no Data Models</p>|
|4|<p>Upload Data - will create new table within the Database that you are connected to</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.019.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.020.png)</p><p>Don’t need to execute this steps, but is great to show for Self Service analytics (Data Scientists & Data Analy</p><p>sts</p>|
|5|<p>Build Data Model</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.021.png)</p><p>Create the Semantic Layer - data is not copied</p>|
|6|<p>Select Table or use SQL</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.022.png)</p><p></p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.023.png)</p><p></p><p></p><p>     </p><p>Ability to add as a table or enter your own pre-defined SQL</p>|
|7|<p>Edit Data Model</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.024.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.025.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.026.png)</p>|
|8|<p>Show Fields quickly</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.027.png)     ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.028.png)</p><p></p><p>Can see that it created fields for each column in the table that was selected.</p><p></p>|
|9|<p>Join Planes table with Flights table</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.029.png)</p><p></p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.030.png)  </p><p></p><p></p><p>` `![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.031.png)</p>|
|10|<p>Join Airlines table with Flights table</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.032.png)  </p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.033.png)</p>|
|11|<p>Preview Data</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.034.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.035.png)</p><p></p><p>Scroll right</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.036.png)</p>|
|12|<p>Edit Fields</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.037.png)</p><p></p><p>Before</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.038.png)</p><p></p><p>You’ll use the following:</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.039.png)    ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.040.png)</p><p></p><p>Edit Field properties</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.041.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.042.png)     ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.043.png)</p><p></p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.044.png)</p><p></p><p>Create New Field</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.045.png) 1st clone</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.046.png)     ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.047.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.048.png)</p><p></p><p>Change Display Name to “Route”</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.049.png)</p><p></p><p>Edit Expression</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.050.png)</p><p></p><p>Expression: </p><p>**concat( [origin],'-', [dest])**</p><p></p><p>Can Validate (to check for any errors) or Click Apply (to accept changes)</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.051.png) or ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.052.png)</p>|
|13|<p>Finished Data Model</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.053.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.054.png)</p><p>Click Save</p>|
|14|<p>Create Dashboard</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.055.png)</p><p>` `![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.056.png)</p>|
|15|<p>First Visual</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.057.png)</p><p></p><p></p><p>Change Dep Delay Aggregate to Average</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.058.png)</p><p></p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.059.png)</p><p></p><p></p><p>Change to only show Top 25 Avgs</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.060.png)</p><p></p><p>Change Alias</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.061.png)</p><p></p><p>Finished</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.062.png)</p><p></p><p>Refresh Visual</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.063.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.064.png)</p><p></p><p>Add Title & Subtitle for Dashboard</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.065.png)</p><p></p><p>Add Title & Subtitle for this chart</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.066.png)</p><p></p>|
|16|<p>Second Visual</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.067.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.068.png)</p><p></p><p>Use Visual Styles to suggest charts</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.069.png)     ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.070.png)</p><p></p><p>Select UniqueCarrier, Cancellationcode, Cancelled</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.071.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.072.png)</p><p></p><p>Filter for only cancelled flights</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.073.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.074.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.075.png)</p><p></p><p>Resize (make larger)</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.076.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.077.png)</p><p></p><p>Add Chart Title - “Cancellation Correlation”</p>|
|17|<p>Add Prompts</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.078.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.079.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.080.png)</p><p></p><p>Select values from prompt</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.081.png)</p><p></p><p></p><p></p>|
|18|<p>Third Visual (optional)</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.082.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.083.png)</p><p></p><p>Depending on user being used you may see the actual data not hashed (would need to login as Analyst user, and view this dashboard)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.084.png) </p><p></p><p>or </p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.085.png)</p><p></p><p>View Tooltips (click Dep Delay value)</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.086.png)</p><p>Use to show the Ranger security policy taking effect</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.087.png)</p>|
|19|<p>Save Dashboard</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.088.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.089.png)</p>|
|20|<p>View Dashboard</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.090.png) click on visuals tab </p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.091.png)</p><p></p><p>Click on Dashboard</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.092.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.093.png)</p>|


||
| :- |

|**Step**|**Description**|||
| :-: | :- | :- | :- |
|1|<p>Open Data Visualization ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.016.png)</p><p></p><p></p><p>SSO login as user with password (not prompted) </p><p></p>|
|2|<p>Overview</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.017.png)</p>|
|3|<p>Switch to Data Tab</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.018.png)</p><p>Initially will have no Data Models</p>|
|4|<p>Upload Data - will create new table within the Database that you are connected to</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.019.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.020.png)</p><p>Don’t need to execute this steps, but is great to show for Self Service analytics (Data Scientists & Data Analy</p><p>sts</p>|
|5|<p>Build Data Model</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.021.png)</p><p>Create the Semantic Layer - data is not copied</p>|
|6|<p>Select Table or use SQL</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.022.png)</p><p></p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.023.png)</p><p></p><p></p><p>     </p><p>Ability to add as a table or enter your own pre-defined SQL</p>|
|7|<p>Edit Data Model</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.024.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.025.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.026.png)</p>|
|8|<p>Show Fields quickly</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.027.png)     ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.028.png)</p><p></p><p>Can see that it created fields for each column in the table that was selected.</p><p></p>|
|9|<p>Join Planes table with Flights table</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.029.png)</p><p></p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.030.png)  </p><p></p><p></p><p>` `![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.031.png)</p>|
|10|<p>Join Airlines table with Flights table</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.032.png)  </p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.033.png)</p>|
|11|<p>Preview Data</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.034.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.035.png)</p><p></p><p>Scroll right</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.036.png)</p>|
|12|<p>Edit Fields</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.037.png)</p><p></p><p>Before</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.038.png)</p><p></p><p>You’ll use the following:</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.039.png)    ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.040.png)</p><p></p><p>Edit Field properties</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.041.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.042.png)     ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.043.png)</p><p></p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.044.png)</p><p></p><p>Create New Field</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.045.png) 1st clone</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.046.png)     ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.047.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.048.png)</p><p></p><p>Change Display Name to “Route”</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.049.png)</p><p></p><p>Edit Expression</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.050.png)</p><p></p><p>Expression: </p><p>**concat( [origin],'-', [dest])**</p><p></p><p>Can Validate (to check for any errors) or Click Apply (to accept changes)</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.051.png) or ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.052.png)</p>|
|13|<p>Finished Data Model</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.053.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.054.png)</p><p>Click Save</p>|
|14|<p>Create Dashboard</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.055.png)</p><p>` `![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.056.png)</p>|
|15|<p>First Visual</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.057.png)</p><p></p><p></p><p>Change Dep Delay Aggregate to Average</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.058.png)</p><p></p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.059.png)</p><p></p><p></p><p>Change to only show Top 25 Avgs</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.060.png)</p><p></p><p>Change Alias</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.061.png)</p><p></p><p>Finished</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.062.png)</p><p></p><p>Refresh Visual</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.063.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.064.png)</p><p></p><p>Add Title & Subtitle for Dashboard</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.065.png)</p><p></p><p>Add Title & Subtitle for this chart</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.066.png)</p><p></p>|
|16|<p>Second Visual</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.067.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.068.png)</p><p></p><p>Use Visual Styles to suggest charts</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.069.png)     ![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.070.png)</p><p></p><p>Select UniqueCarrier, Cancellationcode, Cancelled</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.071.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.072.png)</p><p></p><p>Filter for only cancelled flights</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.073.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.074.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.075.png)</p><p></p><p>Resize (make larger)</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.076.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.077.png)</p><p></p><p>Add Chart Title - “Cancellation Correlation”</p>|
|17|<p>Add Prompts</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.078.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.079.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.080.png)</p><p></p><p>Select values from prompt</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.081.png)</p><p></p><p></p><p></p>|
|18|<p>Third Visual (optional)</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.082.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.083.png)</p><p></p><p>Depending on user being used you may see the actual data not hashed (would need to login as Analyst user, and view this dashboard)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.084.png) </p><p></p><p>or </p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.085.png)</p><p></p><p>View Tooltips (click Dep Delay value)</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.086.png)</p><p>Use to show the Ranger security policy taking effect</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.087.png)</p>|
|19|<p>Save Dashboard</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.088.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.089.png)</p>|
|20|<p>View Dashboard</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.090.png) click on visuals tab </p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.091.png)</p><p></p><p>Click on Dashboard</p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.092.png)</p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.093.png)</p>|

||
| :- |


-----
## (Optional Bonus Material)
1. Run load test to simulate adding many more end users. Then view results of autoscaling. Discuss how autoscaling works, and how it allows for easy, cost-effective scaling. 

|-- Optional step. Can be just a discussion if no load test is actually done.|
| :- |

1. Discuss how workload aware autoscaling runs ETL style queries on dedicated executor group to not interfere with shorter queries. 

|<p>-- Start the creation process for a Hive VW and see the **“Query Isolation” option.**</p><p>-- No need to continue to create the VW.</p>|
| :- |


1. Run example for Data Sketches. 

|<p></p><p>USE DB\_USER001; </p><p></p><p>drop table if exists airlinedata.flights\_airlines\_sk;</p><p></p><p>create table airlinedata.flights\_airlines\_sk as</p><p>select flights\_orc\_partitioned.uniquecarrier AS airline\_code,</p><p>` `count(1) as sum\_flights,</p><p>` `sketch.qt\_data2sketch(cast(arrdelay as double)) as sk\_arrdelay</p><p>FROM airlinedata.flights\_orc</p><p>where arrdelay > 0</p><p>GROUP BY uniquecarrier;</p><p>							</p><p></p><p>select airline\_code,sum\_flights,sum\_arrdelay,</p><p>`	  `sketch.qt\_getPmf(sk\_arrdelay,10,20,30,40,50,60,70,80,90,100),			</p><p>`	  `sketch.qt\_getCdf(sk\_arrdelay,10,20,30,40,50,60,70,80,90,100),</p><p>`	  `sketch.qt\_getK(sk\_arrdelay),						</p><p>`	  `sketch.qt\_getN(sk\_arrdelay)	</p><p>from airlinedata.flights\_airlines\_sk</p><p>group by airline\_code</p><p>			</p><p></p><p></p><p>![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.094.png)</p><p></p><p></p><p></p><p>drop view if exists airlinedata.vw\_flights\_sk\_airlines;</p><p>create view airlinedata.vw\_flights\_sk\_airlines as					</p><p>select airline\_code, </p><p>`   `sum\_flights, sum\_arrdelay, min\_arrdelay, max\_arrdelay, avg\_arrdelay,</p><p>`   `sketch.qt\_getQuantile(sk\_arrdelay,0.1) as arrival\_delay\_10,</p><p>`   `sketch.qt\_getQuantile(sk\_arrdelay,0.2) as arrival\_delay\_20,</p><p>`   `sketch.qt\_getQuantile(sk\_arrdelay,0.3) as arrival\_delay\_30,</p><p>`   `sketch.qt\_getQuantile(sk\_arrdelay,0.4) as arrival\_delay\_40,</p><p>`   `sketch.qt\_getQuantile(sk\_arrdelay,0.5) as arrival\_delay\_50,</p><p>`   `sketch.qt\_getQuantile(sk\_arrdelay,0.6) as arrival\_delay\_60,</p><p>`   `sketch.qt\_getQuantile(sk\_arrdelay,0.7) as arrival\_delay\_70,</p><p>`   `sketch.qt\_getQuantile(sk\_arrdelay,0.8) as arrival\_delay\_80,</p><p>`   `sketch.qt\_getQuantile(sk\_arrdelay,0.9) as arrival\_delay\_90,</p><p>`   `sketch.qt\_getQuantile(sk\_arrdelay,1.0) as arrival\_delay\_100</p><p>from airlinedata.flights\_airlines\_sk;</p><p></p><p>select \* from airlinedata.vw\_flights\_sk\_airlines;</p><p>							</p><p>										  </p>|
| :- |

`             `PDF / CDF - Airline Histogram 

`	`Create Distribution Histograms (here as Excel) 

![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.095.png)


1. Count distinct with HLL algorithm  


|<p>-- Optional step</p><p>-- how many unique flights we have</p><p></p><p>create table airlinedata.flights\_sketch as</p><p>select sketch.hll\_data2sketch( cast(concat(flights\_orc.uniquecarrier,flights\_orc.flightnum) as string) ) AS flightnum\_sk</p><p>FROM airlinedata.flights\_orc;</p><p></p><p>select sketch.hll\_estimate(flightnum\_sk)</p><p>from airlinedata.flights\_sketch;</p><p></p><p>Results</p><p>     </p>|
| :- |

|44834.13712876354|
| :- |

||
| :- |

Explain - extreme fast query a table 

![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.096.png)

|<p>-- alternative classic query would be</p><p>select count(distinct(cast(concat(flights\_orc.uniquecarrier,flights\_orc.flightnum) as string))) from airlinedata.flights\_orc;</p><p></p><p></p><p></p><p>Results</p><p>     </p>|
| :- |

|44684|
| :- |

||
| :- |

Explain - query full fact table with going over 86mio of the fact table

![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.097.png)

![](Aspose.Words.10bb90cf-0d99-47f3-a995-23ef2b90be86.098.png)

1. Needle in Haystack - most frequency items - or better not on Alaska Airline Flight - AS65




|<p>-- Optional step. Can be just a discussion</p><p>-- What flights are most frequently cancelled </p><p></p><p>drop table if exists airlinedata.flights\_frq\_sketch; 					  </p><p>create table airlinedata.flights\_frq\_sketch (cancelled int, sk\_flightnum binary);</p><p>insert into airlinedata.flights\_frq\_sketch </p><p>select flights\_orc.cancelled, sketch.frq\_data2sketch( cast(concat(flights\_orc.uniquecarrier,flights\_orc.flightnum) as string), 8192 )</p><p>FROM airlinedata.flights\_orc</p><p>GROUP BY flights\_orc.cancelled;</p><p>							</p><p>-- frq\_get\_item is a UDTF . resultset is a sorted table</p><p>-- </p><p>select sketch.frq\_get\_items(sk\_flightnum, 'NO\_FALSE\_POSITIVES')</p><p>from airlinedata.flights\_frq\_sketch;</p><p></p><p></p><p>Results</p><p>     </p>|
| :- |

|ITEM|ESTIMATE|LOWER\_BOUND|UPPER\_BOUND|
| :- | :- | :- | :- |
|AS65|957|586|957|
|WN25|929|558|929|
|AS64|884|513|884|

||
| :- |





|<p>-- validate the result</p><p></p><p>select concat(flights\_orc.uniquecarrier,flights\_orc.flightnum) as flight, </p><p>`       `count(1) as num\_cancelled</p><p>from airlinedata.flights\_orc </p><p>where flightnum = 65 and cancelled = 1</p><p>group by concat(flights\_orc.uniquecarrier,flights\_orc.flightnum)</p><p>order by num\_cancelled desc;</p><p>					  </p><p></p><p>Results</p><p>     </p>|
| :- |

|FLIGHT|NUM\_CANCELLED|
| :- | :- |
|AS65|940|
|TW65|111|
|US65|74|

||
| :- |



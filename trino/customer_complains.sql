create schema postgresdb.airlinedata;

DROP TABLE IF EXISTS postgresdb.airlinedata.customer_complaints;

CREATE TABLE postgresdb.airlinedata.customer_complaints AS
SELECT 
    row_number() OVER () as complaint_id,
    
-- 1. Construct the complaint_date safely
CAST(
    year || '-' || 
    LPAD(month, 2, '0') || '-' || 
    LPAD(dayofmonth, 2, '0') || ' ' || 
    CASE 
        WHEN LPAD(deptime, 4, '0') = '2400' THEN '00:00:00'
        ELSE 
            LPAD(SUBSTR(LPAD(deptime, 4, '0'), 1, 2), 2, '0') || ':' || 
            LPAD(SUBSTR(LPAD(deptime, 4, '0'), 3, 2), 2, '0') || ':00'
    END
AS TIMESTAMP) as complaint_date,

    -- 2. Realistic Emails
    (CASE (row_number() OVER () % 5)
        WHEN 0 THEN 'j.smith'
        WHEN 1 THEN 'm.garcia'
        WHEN 2 THEN 'alex.chen'
        WHEN 3 THEN 'sarah_j'
        ELSE 'kb_traveler'
    END) || CAST(row_number() OVER () AS VARCHAR) || '@' ||
    (CASE (row_number() OVER () % 4)
        WHEN 0 THEN 'gmail.com'
        WHEN 1 THEN 'outlook.com'
        WHEN 2 THEN 'icloud.com'
        ELSE 'yahoo.com'
    END) as customer_email,

    -- 3. Categories & Text
    CASE 
        WHEN cancelled = '1' THEN 'Involuntary Cancellation'
        WHEN try_cast(arrdelay AS INTEGER) > 180 THEN 'DOT Refund Eligible Delay'
        WHEN try_cast(nasdelay AS INTEGER) > 45 THEN 'Carrier/ATC Congestion'
        ELSE 'Service Dissatisfaction'
    END as complaint_category,
    
    CASE 
        WHEN cancelled = '1' THEN 'Flight ' || flightnum || ' was cancelled. I am stuck at ' || origin || ' and the rebooking app is crashing.'
        WHEN try_cast(arrdelay AS INTEGER) > 180 THEN 'Sitting on the tarmac for hours. This violates the 3-hour domestic rule.'
        ELSE 'The cabin crew on ' || uniquecarrier || ' was unresponsive to my requests.'
    END as complaint_text,

    uniquecarrier,
    flightnum,
    try_cast(arrdelay AS INTEGER) as delay_minutes,
    CASE 
        WHEN cancelled = '1' THEN 5 
        ELSE (CASE WHEN try_cast(arrdelay AS INTEGER) > 120 THEN 4 ELSE 2 END)
    END as severity_score

FROM hive.${your_dbname}.flights_csv
WHERE (try_cast(arrdelay AS INTEGER) > 60 OR cancelled = '1')
LIMIT 50000;



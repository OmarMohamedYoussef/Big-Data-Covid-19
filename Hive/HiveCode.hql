
CREATE database if NOT EXISTS covid_db;

use covid_db;

-- Create Staging Table
CREATE TABLE IF NOT EXISTS covid_staging (
    Country STRING,
    Total_Cases DOUBLE,
    New_Cases DOUBLE,
    Total_Deaths DOUBLE,
    New_Deaths DOUBLE,
    Total_Recovered DOUBLE,
    Active_Cases DOUBLE,
    Serious DOUBLE,
    Tot_Cases DOUBLE,
    Deaths DOUBLE,
    Total_Tests DOUBLE,
    Tests DOUBLE,
    CASES_per_Test DOUBLE,
    Death_in_Closed_Cases STRING,
    Rank_by_Testing_rate DOUBLE,
    Rank_by_Death_rate DOUBLE,
    Rank_by_Cases_rate DOUBLE,
    Rank_by_Death_of_Closed_Cases DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/cloudera/ds/COVID_HDFS_LZ'
TBLPROPERTIES ("skip.header.line.count"="1");

------------------------------------------
-- Create Partitioned Table
CREATE EXTERNAL TABLE IF NOT EXISTS covid_ds_partitioned (
    Country STRING,
    Total_Cases DOUBLE,
    New_Cases DOUBLE,
    Total_Deaths DOUBLE,
    New_Deaths DOUBLE,
    Total_Recovered DOUBLE,
    Active_Cases DOUBLE,
    Serious DOUBLE,
    Tot_Cases DOUBLE,
    Deaths DOUBLE,
    Total_Tests DOUBLE,
    Tests DOUBLE,
    CASES_per_Test DOUBLE,
    Death_in_Closed_Cases STRING,
    Rank_by_Testing_rate DOUBLE,
    Rank_by_Death_rate DOUBLE,
    Rank_by_Cases_rate DOUBLE,
    Rank_by_Death_of_Closed_Cases DOUBLE
)
PARTITIONED BY (COUNTRY_NAME STRING)
STORED AS ORC
LOCATION '/user/cloudera/ds/COVID_HDFS_PARTITIONED'
SET hive.exec.dynamic.partition.mode=nonstrict;

-- OR 

SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE EXTERNAL TABLE IF NOT EXISTS covid_ds_partitioned (
    Country STRING,
    Total_Cases DOUBLE,
    New_Cases DOUBLE,
    Total_Deaths DOUBLE,
    New_Deaths DOUBLE,
    Total_Recovered DOUBLE,
    Active_Cases DOUBLE,
    Serious DOUBLE,
    Tot_Cases DOUBLE,
    Deaths DOUBLE,
    Total_Tests DOUBLE,
    Tests DOUBLE,
    CASES_per_Test DOUBLE,
    Death_in_Closed_Cases STRING,
    Rank_by_Testing_rate DOUBLE,
    Rank_by_Death_rate DOUBLE,
    Rank_by_Cases_rate DOUBLE,
    Rank_by_Death_of_Closed_Cases DOUBLE
)
PARTITIONED BY (COUNTRY_NAME STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
LOCATION '/user/cloudera/ds/COVID_HDFS_PARTITIONED';

-------------------------------------------------
CREATE TABLE IF NOT EXISTS covid_staging2 (
    Country STRING,
    Total_Cases DOUBLE,
    New_Cases DOUBLE,
    Total_Deaths DOUBLE,
    New_Deaths DOUBLE,
    Total_Recovered DOUBLE,
    Active_Cases DOUBLE,
    Serious DOUBLE,
    Tot_Cases DOUBLE,
    Deaths DOUBLE,
    Total_Tests DOUBLE,
    Tests DOUBLE,
    CASES_per_Test DOUBLE,
    Death_in_Closed_Cases STRING,
    Rank_by_Testing_rate DOUBLE,
    Rank_by_Death_rate DOUBLE,
    Rank_by_Cases_rate DOUBLE,
    Rank_by_Death_of_Closed_Cases DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/cloudera/ds/COVID_HDFS_LZ'
TBLPROPERTIES ("skip.header.line.count"="51");


CREATE TABLE IF NOT EXISTS covid_staging1 (
    Country STRING,
    Total_Cases DOUBLE,
    New_Cases DOUBLE,
    Total_Deaths DOUBLE,
    New_Deaths DOUBLE,
    Total_Recovered DOUBLE,
    Active_Cases DOUBLE,
    Serious DOUBLE,
    Tot_Cases DOUBLE,
    Deaths DOUBLE,
    Total_Tests DOUBLE,
    Tests DOUBLE,
    CASES_per_Test DOUBLE,
    Death_in_Closed_Cases STRING,
    Rank_by_Testing_rate DOUBLE,
    Rank_by_Death_rate DOUBLE,
    Rank_by_Cases_rate DOUBLE,
    Rank_by_Death_of_Closed_Cases DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/cloudera/ds/COVID_HDFS_LZ'
TBLPROPERTIES ("skip.header.line.count"="101");


CREATE TABLE IF NOT EXISTS covid_staging3 (
    Country STRING,
    Total_Cases DOUBLE,
    New_Cases DOUBLE,
    Total_Deaths DOUBLE,
    New_Deaths DOUBLE,
    Total_Recovered DOUBLE,
    Active_Cases DOUBLE,
    Serious DOUBLE,
    Tot_Cases DOUBLE,
    Deaths DOUBLE,
    Total_Tests DOUBLE,
    Tests DOUBLE,
    CASES_per_Test DOUBLE,
    Death_in_Closed_Cases STRING,
    Rank_by_Testing_rate DOUBLE,
    Rank_by_Death_rate DOUBLE,
    Rank_by_Cases_rate DOUBLE,
    Rank_by_Death_of_Closed_Cases DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/cloudera/ds/COVID_HDFS_LZ'
TBLPROPERTIES ("skip.header.line.count"="151");



CREATE TABLE IF NOT EXISTS covid_staging4 (
    Country STRING,
    Total_Cases DOUBLE,
    New_Cases DOUBLE,
    Total_Deaths DOUBLE,
    New_Deaths DOUBLE,
    Total_Recovered DOUBLE,
    Active_Cases DOUBLE,
    Serious DOUBLE,
    Tot_Cases DOUBLE,
    Deaths DOUBLE,
    Total_Tests DOUBLE,
    Tests DOUBLE,
    CASES_per_Test DOUBLE,
    Death_in_Closed_Cases STRING,
    Rank_by_Testing_rate DOUBLE,
    Rank_by_Death_rate DOUBLE,
    Rank_by_Cases_rate DOUBLE,
    Rank_by_Death_of_Closed_Cases DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/cloudera/ds/COVID_HDFS_LZ'
TBLPROPERTIES ("skip.header.line.count"="201");
---------------------------------------------------------
-- Step 5: Insert Data into Partitioned Table
INSERT INTO covid_ds_partitioned PARTITION (COUNTRY_NAME)
SELECT 
    Country,
    Total_Cases,
    New_Cases,
    Total_Deaths,
    New_Deaths,
    Total_Recovered,
    Active_Cases,
    Serious,
    Tot_Cases,
    Deaths,
    Total_Tests,
    Tests,
    CASES_per_Test,
    Death_in_Closed_Cases,
    Rank_by_Testing_rate,
    Rank_by_Death_rate,
    Rank_by_Cases_rate,
    Rank_by_Death_of_Closed_Cases,
    Country as COUNTRY_NAME -- Dynamic partition column
FROM
    covid_staging
WHERE
    Country IS NOT NULL
    LIMIT 50;

INSERT INTO covid_ds_partitioned PARTITION (COUNTRY_NAME)
SELECT 
    Country,
    Total_Cases,
    New_Cases,
    Total_Deaths,
    New_Deaths,
    Total_Recovered,
    Active_Cases,
    Serious,
    Tot_Cases,
    Deaths,
    Total_Tests,
    Tests,
    CASES_per_Test,
    Death_in_Closed_Cases,
    Rank_by_Testing_rate,
    Rank_by_Death_rate,
    Rank_by_Cases_rate,
    Rank_by_Death_of_Closed_Cases,
    Country as COUNTRY_NAME -- Dynamic partition column
FROM
    covid_staging2
WHERE
    Country IS NOT NULL
    LIMIT 50;

INSERT INTO covid_ds_partitioned PARTITION (COUNTRY_NAME)
SELECT 
    Country,
    Total_Cases,
    New_Cases,
    Total_Deaths,
    New_Deaths,
    Total_Recovered,
    Active_Cases,
    Serious,
    Tot_Cases,
    Deaths,
    Total_Tests,
    Tests,
    CASES_per_Test,
    Death_in_Closed_Cases,
    Rank_by_Testing_rate,
    Rank_by_Death_rate,
    Rank_by_Cases_rate,
    Rank_by_Death_of_Closed_Cases,
    Country as COUNTRY_NAME -- Dynamic partition column
FROM
    covid_staging1
WHERE
    Country IS NOT NULL
    LIMIT 50;

INSERT INTO covid_ds_partitioned PARTITION (COUNTRY_NAME)
SELECT 
    Country,
    Total_Cases,
    New_Cases,
    Total_Deaths,
    New_Deaths,
    Total_Recovered,
    Active_Cases,
    Serious,
    Tot_Cases,
    Deaths,
    Total_Tests,
    Tests,
    CASES_per_Test,
    Death_in_Closed_Cases,
    Rank_by_Testing_rate,
    Rank_by_Death_rate,
    Rank_by_Cases_rate,
    Rank_by_Death_of_Closed_Cases,
    Country as COUNTRY_NAME -- Dynamic partition column
FROM
    covid_staging3
WHERE
    Country IS NOT NULL
    LIMIT 50;

INSERT INTO covid_ds_partitioned PARTITION (COUNTRY_NAME)
SELECT 
    Country,
    Total_Cases,
    New_Cases,
    Total_Deaths,
    New_Deaths,
    Total_Recovered,
    Active_Cases,
    Serious,
    Tot_Cases,
    Deaths,
    Total_Tests,
    Tests,
    CASES_per_Test,
    Death_in_Closed_Cases,
    Rank_by_Testing_rate,
    Rank_by_Death_rate,
    Rank_by_Cases_rate,
    Rank_by_Death_of_Closed_Cases,
    Country as COUNTRY_NAME -- Dynamic partition column
FROM
    covid_staging4
WHERE
    Country IS NOT NULL
    LIMIT 16;

-- Create Final Output Table
CREATE EXTERNAL TABLE IF NOT EXISTS covid_final_output (
    TOP_DEATH STRING,
    TOP_TEST STRING
)
PARTITIONED BY (COUNTRY_NAME STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/cloudera/ds/COVID_FINAL_OUTPUT';

SELECT * FROM covid_staging



CREATE EXTERNAL TABLE IF NOT EXISTS covid_final_output (
    TOP_DEATH STRING,
    TOP_TEST STRING
)
PARTITIONED BY (COUNTRY_NAME STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/cloudera/ds/COVID_FINAL_OUTPUT';


INSERT INTO covid_final_output PARTITION (COUNTRY_NAME)
SELECT 
    MAX(Country) AS TOP_DEATH,
    MAX(Country) AS TOP_TEST,
    COUNTRY_NAME
FROM
    covid_ds_partitioned
GROUP BY
    COUNTRY_NAME;



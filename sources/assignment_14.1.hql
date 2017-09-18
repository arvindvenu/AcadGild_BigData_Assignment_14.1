-- to know which DB is being used currently
set hive.cli.print.current.db=true;

-- create a database named custom on HDFS
CREATE DATABASE IF NOT EXISTS `custom`;

-- switch to the database custom which we created
USE custom;

-- create a table called temperature_data_temp
-- this will be used to load the data. It is required to create this
-- temp table because date field cannot be read directly
CREATE TABLE IF NOT EXISTS `temperature_data_temp`(
	temperature_date string,
	zip_code int,
	temperature float
)
row format delimited
fields terminated by ',';
 
-- this is the actual table into which the data will be loaded
CREATE TABLE IF NOT EXISTS `temperature_data`(
	temperature_date date,
	zip_code int,
	temperature float
);

-- load the data to the temp table
LOAD DATA 
LOCAL INPATH '/home/arvind/hive/acadgild/assignments/assignment_14.1/input/input.txt' OVERWRITE
INTO TABLE temperature_data_temp;

-- load the data to the actual table from the temp table
-- IN this process convert the date in string format to date format
INSERT INTO temperature_data 
SELECT TO_DATE(from_unixtime(unix_timestamp(temperature_date, 'MM-dd-yyyy'))), zip_code, temperature FROM temperature_data_temp;

-- drop the temp table
DROP TABLE IF EXISTS temperature_data_temp;

-- store the output in a directory in the local file system
INSERT OVERWRITE LOCAL DIRECTORY '/home/arvind/hive/acadgild/assignments/assignment_14.1/output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
SELECT * FROM temperature_data;



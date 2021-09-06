--creating the databese
create database sparkdb;

--creating the table in Mysql the upload in databricks
walmart_stock_2_csv
create table
--Query:
select * from sparkdb.walmart_stock_2_csv limit 20;



--"What are the column names?"

select Date,Open,High,Low,Close,Volume from sparkdb.walmart_stock_2_csv;




--What does the Schema look like?"

describe sparkdb.walmart_stock_2_csv;
describe formatted sparkdb.walmart_stock_2_csv;


--Print out the first 5 columns."
select * from sparkdb.walmart_stock_2_csv limit 5;


--## Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day."
SELECT avg(High) as avg_high,avg(Volume) as avg_Volume from sparkdb.walmart_stock_2_csv;

--#What day had the Peak High in Price?"

select Date,High as max_high from sparkdb.walmart_stock_2_csv where High=(select max(High) from sparkdb.walmart_stock_2_csv) ;


--#What is the mean of the Close column?"

select mean(Close) as mean_close from sparkdb.walmart_stock_2_csv;


--## What is the max and min of the Volume column?"
select max(Volume),min(Volume) from sparkdb.walmart_stock_2_csv;


--# How many days was the Close lower than 60 dollars?"
select count(Date) from sparkdb.walmart_stock_2_csv where Close>60;

select count(Date) from sparkdb.walmart_stock_2_csv where Close<60;



--#What percentage of the time was the High greater than 80 dollars ?\n"
select count(*)/(select count(*) from sparkdb.walmart_stock_2_csv)*100 as High_percentage from sparkdb.walmart_stock_2_csv where High>80;

select max(Date) from sparkdb.walmart_stock_2_csv where High<80;

select min(High) from sparkdb.walmart_stock_2_csv where High<80;



--#What is the Pearson correlation between High and Volume?\n"
select corr(High,Volume) from sparkdb.walmart_stock_2_csv;




--#What is the max High per year?"
select SUBSTRING(Date,0,4) as year,max(High) as max_high from sparkdb.walmart_stock_2_csv group by year;

--select max(High) from sparkdb.walmart_stock_2_csv where Date between 01-02-2012 and 29-02-2012;



--#What is the average Close for each Calendar Month?\n",

select SUBSTRING(Date,6,2) as month,avg(Close) as max_high from sparkdb.walmart_stock_2_csv group by month order by month;


create database demodb;
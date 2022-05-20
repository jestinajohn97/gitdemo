-- Case Study on Flights Delay Analysis
-- 1. Create a Table Flights with schemas of Table

create database if not exists flightDB;
use flightDB;

drop table if exists flight;
create table if not exists flight(ID SERIAL,YEAR int,MONTH int,DAY int,DAY_OF_WEEK int,AIRLINE varchar(10),FLIGHT_NUMBER int,TAIL_NUMBER varchar(20),ORIGIN_AIRPORT varchar(10),DESTINATION_AIRPORT varchar(20),SCHEDULED_DEPARTURE int,DEPARTURE_TIME int,DEPARTURE_DELAY int,TAXI_OUT int,WHEELS_OFF int,SCHEDULED_TIME	int,ELAPSED_TIME int,AIR_TIME int,DISTANCE int,WHEELS_ON int,TAXI_IN int,SCHEDULED_ARRIVAL int,ARRIVAL_TIME int,ARRIVAL_DELAY int,DIVERTED int,CANCELLED int,CANCELLATION_REASON int,AIR_SYSTEM_DELAY int,SECURITY_DELAY int,AIRLINE_DELAY int,LATE_AIRCRAFT_DELAY int,WEATHER_DELAY int);


-- 2. Insert all records into flights table. Use dataset Flights_Delay.csv
describe flight;
SET GLOBAL local_infile = true;
LOAD DATA LOCAL INFILE 'E:/Data/mysql/Flights_Delay.csv' INTO 
TABLE flight FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

-- Write a MySQL Queries to display the results
-- 3.Average Arrival delay caused by airlines 
select AIRLINE,avg(ARRIVAL_DELAY) AS AVERAGE_DELAY from flight group by AIRLINE;

-- 4. Display the Day of Month with AVG Delay [Hint: Add Count() of Arrival & Departure Delay]
select MONTH,avg(ARRIVAL_DELAY),avg(DEPARTURE_DELAY)  from flight group by MONTH ORDER BY MONTH;

-- 5. Analysis for each month with total number of cancellations.
select AIRLINE,MONTH,sum(CANCELLED) AS TOTAL_CANCELLATION from flight group by MONTH;

-- 6. Find the airlines that make maximum number of cancellations
select AIRLINE,max(CANCELLED) AS MAX_CANCELLED from flight group by AIRLINE;

-- 7. Finding the Busiest Airport [Hint: Find Count() of origin airport and destination airport]
select ORIGIN_AIRPORT,COUNT(ORIGIN_AIRPORT) AS ORG_AIRPORT from flight group by ORIGIN_AIRPORT ORDER BY ORG_AIRPORT DESC;
select DESTINATION_AIRPORT,COUNT(ORIGIN_AIRPORT) AS DES_AIRPORT from flight group by DESTINATION_AIRPORT ORDER BY DES_AIRPORT DESC;
-- 8. Find the airlines that make maximum number of Diversions [Hint: Diverted = 1 indicate Diversion]
select AIRLINE, max(DIVERTED) as TOTAL_DIVERSIONS from flight WHERE DIVERTED=1 group by AIRLINE;

-- 9. Finding all diverted Route from a source to destination Airport & which route is the most diverted route.
SELECT ORIGIN_AIRPORT,DESTINATION_AIRPORT,DIVERTED from flight where DIVERTED=1 order by DIVERTED desc;

-- 10. Finding all Route from origin to destination Airport & which route got delayed. 
SELECT ORIGIN_AIRPORT,DESTINATION_AIRPORT,ARRIVAL_DELAY from flight where ARRIVAL_DELAY >0;

-- 11. Finding the Route which Got Delayed the Most [Hint: Route include Origin Airport and Destination Airport, Group By Both ]
SELECT ORIGIN_AIRPORT,DESTINATION_AIRPORT,sum(ARRIVAL_DELAY) as most from flight  GROUP BY ORIGIN_AIRPORT, DESTINATION_AIRPORT order by most DESC;

-- 12. Finding AIRLINES with its total flight count, total number of flights arrival delayed by more than 30 Minutes, % of such flights delayed by more than 30 minutes when it is not Weekends with minimum count of flights from Airlines by more than 10. Also Exclude some of Airlines 'AK', 'HI', 'PR', 'VI' and arrange output in descending order by % of such count of flights.



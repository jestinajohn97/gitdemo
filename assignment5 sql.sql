create database if not exists hr_attribution_db;
USE hr_attribution_db;
drop table if exists hr_attribution;
create table if not exists hr_attribution(EmployeeID SERIAL,Department varchar(50),JobRole varchar(50),Attrition varchar(20),Gender varchar(20),Age int,MaritalStatus varchar(30),Education	varchar(30),EducationField varchar(50),BusinessTravel varchar(20),JobInvolvement varchar(30),JobLevel int,JobSatisfaction varchar(30),Hourlyrate int,Income int,Salaryhike int,OverTime varchar(20),Workex	int,YearsSinceLastPromotion	 int,EmpSatisfaction varchar(20),TrainingTimesLastYear int,WorkLifeBalance	varchar(30),Performance_Rating varchar(20),primary key (EmployeeID));
describe hr_attribution;
SET GLOBAL local_infile = true;
show tables;
LOAD DATA LOCAL INFILE 'E:/Data/mysql/HR_Employee.csv' INTO 
TABLE hr_attribution FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

select * from hr_attribution limit 5;

-- 1. Shape of table
-- Returns Number of Rows
select count(EmployeeID) from hr_attribution;

-- Returns Number of Columns      
select count(*) from information_schema.columns where table_name = 'hr_attribution';
-- select * from information_schema.columns where table_name = 'hr_attribution';


-- 2. Show the count of Employee & percentage Workforce in each Department.
select department, count(*) as COUNT_EMP from hr_attribution GROUP BY department ORDER BY COUNT_EMP DESC;


select department, count(*) as Count_EMP, count(*)*100/(select count(*) from hr_attribution) 
as percentage_dept from hr_attribution group by department order by Count_EMP desc;

-- 3. Which gender have higher strength as workforce in each department ?
select department, gender, count(*) as COUNT_EMP from hr_attribution group by department, gender ORDER BY COUNT_EMP;

-- 4. Show the workforce in each JobRole
-- Observation: 

select JobRole, count(*) as COUNT_EMP from hr_attribution group by JobRole  ORDER BY COUNT_EMP desc;

-- 5. Show Distribution of Employee's Age Group
-- ALTER TABLE HR_Employee DROP age_group;
 alter table hr_attribution add column age_group varchar(20);
 
 SET SQL_SAFE_UPDATES = 0;
 
 -- Assign Values to age_group 
 update hr_attribution 
 SET age_group = if ( age <= 25, '<25', if (age > 40, '40+', '25-40'));
 
 select age_group, COUNT(*) as emp_num from hr_attribution GROUP BY age_group;

-- 6. compare all marital status of employee and find the most frequent marital status

select MaritalStatus, count(*) as marital_count from hr_attribution group by MaritalStatus  ORDER BY marital_count desc;

-- 7 . what is job satisfaction level of employee
select JobSatisfaction, count(*) as Count_JobSatisfy, count(*)*100/(select count(*) from hr_attribution) 
as percent_JobSatisfy from hr_attribution group by JobSatisfaction order by Count_JobSatisfy desc;

-- 8. How frequently employee are going on Business Trip
select BusinessTravel, count(*) as Count_Trip, count(*)*100/(select count(*) from hr_attribution) 
as Percent_Trip from hr_attribution group by BusinessTravel order by Percent_Trip desc;

-- 9. Show the Department with Highest Attrition Rate (Percentage)
select Department,Attrition, count(*) as Count_EMP, count(*)*100/(select count(*) from hr_attribution) 
as Percent_EMP from hr_attribution group by Department,Attrition order by Percent_EMP desc;

select department, sum(case when attrition = 'Yes' then 1 else 0 end) as count_attr,
round(sum(case when attrition = 'Yes' then 1 else 0 end)*100/count(*), 2) as attr_rate from hr_attribution group by
department;

-- 10. Show the JobRole with Highest Attrition Rate (Percentage)
select JobRole,Attrition, count(*) as Count_EMP, count(*)*100/(select count(*) from hr_attribution) 
as Percent_EMP from hr_attribution group by JobRole,Attrition order by Percent_EMP desc;

select JobRole, sum(case when attrition = 'Yes' then 1 else 0 end) as count_attr,
round(sum(case when attrition = 'Yes' then 1 else 0 end)*100/count(*), 2) as attr_rate from hr_attribution group by
JobRole;
  
-- 11. Show Distribution of Employee's Promotion, Find the maximum chances of employee
-- getting Promoted
-- YearsSinceLastPromotion
select DISTINCT(YearsSinceLastPromotion) from hr_attribution;
alter table hr_attribution drop column promotion_group;
alter table hr_attribution add column promotion_group varchar(30);

SET SQL_SAFE_UPDATES = 0;

update hr_attribution
SET promotion_group = if ( YearsSinceLastPromotion <= 5, '<=5', 
if (YearsSinceLastPromotion > 10, '10+', '6-10')); 
select promotion_group, COUNT(*) as count_num from hr_attribution GROUP BY promotion_group;

-- 12. Find the Atrrition Rate for Marital Status
-- Observation : Highest Attrition is in Singles 
select MaritalStatus, Attrition, count(*) as Count_EMP, count(*)*100/(select count(*) from hr_attribution) 
as Percent_EMP from hr_attribution group by MaritalStatus,Attrition order by Percent_EMP desc;

-- 13. Find the Attrition Count & Percentage for Different Education Levels
-- Observation: Higher Education have Lower Attrition Rate
select education, Attrition, count(*) as Count_EMP, count(*)*100/(select count(*) from hr_attribution) 
as Percent_EMP from hr_attribution group by education,Attrition order by Percent_EMP desc;


-- 14. Find the Attrition & Percentage Attrition for Business Travel 
-- Observation: Attrition is High for Employees Travelling Frquently
select BusinessTravel, sum(case when attrition = 'Yes' then 1 else 0 end) as count_attr, 
round(SUM(if(attrition = 'yes', 1, 0))/count(*), 2) as attr_rate from hr_attribution group by BusinessTravel;

-- 15. Find the Attrition & Percentage Attrition for Various JobInvolvement
-- Observation: Low Job Involvement Leads to High Attrition Rate
select JobInvolvement, sum(case when attrition = 'Yes' then 1 else 0 end) as count_attr, 
round(SUM(if(attrition = 'yes', 1, 0))/count(*), 2) as attr_rate from hr_attribution  
group by JobInvolvement ORDER BY attr_rate;

  
-- 16. Show Attrition Rate for Different JobSatisfaction
-- Observation: Low Job Satisafaction leads to High Attrition Rate
  
  select JobSatisfaction, sum(case when attrition = 'Yes' then 1 else 0 end) as count_attr, 
round(SUM(if(attrition = 'yes', 1, 0))/count(*), 2) as attr_rate from hr_attribution  
group by JobSatisfaction ORDER BY attr_rate;
  

-- ACTIVITIES
Use bank;
/*
3.07 Activity 1
Keep working on the bank database.

Modify the previous query to obtain the percentage of variation in the number of users compared with previous month.

-- the previous query:
with cte_activity as (
  select Active_users, lag(Active_users,1) over (partition by Activity_year) as last_month, Activity_year, Activity_month
  from Monthly_active_users
)
select * from cte_activity
where last_month is not null;
*/
/*
with cte_activity as (
  select Active_users, lag(Active_users,1) over (partition by Activity_year) as last_month, Activity_year, Activity_month
  from Monthly_active_users
)
select * from cte_activity
where last_month is not null;
--
with cte_activity as (select Active_users, 
						lag(Active_users,1) over (partition by Activity_year) as last_month, 
						Activity_year, 
						Activity_month,
						(Active_users - Last_month) as Difference,
						(Active_users - Last_month) / Active_users * 100 AS perc_variation
					from Monthly_active_users
					)
--
--*/
-- Step 1: Get the account_id, date, year, month and month_number for
-- every transaction.
use bank;
drop view if exists user_activity; 
create or replace view user_activity as
select account_id, convert(date, date) as Activity_date,
date_format(convert(date,date), '%M') as Activity_Month,
date_format(convert(date,date), '%m') as Activity_Month_number,
date_format(convert(date,date), '%Y') as Activity_year
from bank.trans;

-- Checking results
select * from bank.user_activity;

-- Step 2:
-- Computing the total number of active users by Year and Month with group by
-- and sorting according to year and month NUMBER.
select Activity_year, Activity_Month, count(account_id) as Active_users 
from bank.user_activity
group by Activity_year, Activity_Month
order by Activity_year asc;

-- Step 3:
-- Storing the results on a view for later use.
use bank;
drop view if exists bank.monthly_active_users;
create view bank.monthly_active_users as
select Activity_year, Activity_Month, count(account_id) as Active_users 
from bank.user_activity
group by Activity_year, Activity_Month
order by Activity_year asc;

-- Sanity check
select * from bank.monthly_active_users;


/*
-- Final step:
Compute the difference of `active_users` between one month and the previous one
for each year
using the lag function with lag = 1 (as we want the lag from one previous record)
*/

select 
   Activity_year, 
   Activity_month,
   Active_users, 
   lag(Active_users,1) over (order by Activity_year) as Last_month
from monthly_active_users;

-- Refining: Getting the difference of monthly active_users month to month.

with cte_activity as (select Active_users, 
						lag(Active_users,1) over (partition by Activity_year) as last_month, 
						Activity_year, 
						Activity_month,
						(Active_users - lag(Active_users,1) over (partition by Activity_year)) as Difference,
						(Active_users - lag(Active_users,1) over (partition by Activity_year)) / Active_users * 100 AS perc_variation
					from Monthly_active_users
					)
SELECT * FROM cte_activity
-- CREO ESTE ESTÁ CORRECTO
-- Step 1: Get the account_id, date, year, month and month_number for
-- every transaction.
use bank;
drop view if exists user_activity; 
create or replace view user_activity as
select account_id, convert(date, date) as Activity_date,
date_format(convert(date,date), '%M') as Activity_Month,
date_format(convert(date,date), '%m') as Activity_Month_number,
date_format(convert(date,date), '%Y') as Activity_year
from bank.trans;

-- Checking results
select * from bank.user_activity;

-- Step 2:
-- Computing the total number of active users by Year and Month with group by
-- and sorting according to year and month NUMBER.
select Activity_year, Activity_Month, count(account_id) as Active_users 
from bank.user_activity
group by Activity_year, Activity_Month, Activity_Month_number
order by Activity_year asc,Activity_Month_number asc;

-- Step 3:
-- Storing the results on a view for later use.
drop view bank.monthly_active_users;
create view bank.monthly_active_users as
select Activity_year, Activity_Month, Activity_Month_number, count(account_id) as Active_users from bank.user_activity
group by Activity_year, Activity_Month, Activity_Month_number
order by Activity_year asc, Activity_Month_number asc;

-- Sanity check
select * from monthly_active_users;


/*
-- Final step:
Compute the difference of `active_users` between one month and the previous one
for each year
using the lag function with lag = 1 (as we want the lag from one previous record)
*/

select 
   Activity_year, 
   Activity_month,
   Activity_month_number,
   Active_users, 
   lag(Active_users,1) over (order by Activity_year, Activity_Month_number) as Last_month
from monthly_active_users;

-- Refining: Getting the difference of monthly active_users month to month.
with cte_view as (select 
   Activity_year, 
   Activity_month,
   Active_users, 
   lag(Active_users,1) over (order by Activity_year, Activity_Month_number) as Last_month
from monthly_active_users)
select Activity_year, Activity_month, Active_users, Last_month, (Active_users - Last_month) as Difference from cte_view;

#select * from Monthly_active_users

with cte_activity as (select Active_users, 
						lag(Active_users,1) over (order by Activity_year) as last_month, 
						Activity_year, 
						Activity_month,
						(Active_users - lag(Active_users,1) over (order by Activity_year)) as Difference,
						(Active_users - lag(Active_users,1) over (order by Activity_year)) / Active_users * 100 AS perc_variation
					from Monthly_active_users
					)
SELECT * FROM cte_activity
where difference is not null;
--
/*
3.07 Activity 2
Modify the previous queries to list the customers lost last month.
*/
/*
Getting the total number of UNIQUE active customers for each year-month.
*/
SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));
/*
Getting the total number of UNIQUE active customers for each year-month.
*/

-- Step 1: Get the unique account_id every year, month, and month_number
select distinct account_id as Active_id, Activity_year, Activity_month, Activity_month_number from bank.user_activity;

--  Step 2: Create a view with the previous information
drop view bank.distinct_users;
create view bank.distinct_users as
select distinct account_id as Active_id, Activity_year, Activity_month, Activity_month_number from bank.user_activity;

-- Check results
select * from bank.distinct_users;

-- Final step: Do a cross join for the previous view but with the following restrictions:
-- 1)-The Active_id MUST exist in the second table
-- 2)-The Activity_month should be shifted by one.
select 
   d1.Activity_year,
   d1.Activity_month,
   d1.Activity_month_number,
   count(distinct d1.Active_id) as Retained_customers
   from bank.distinct_users as d1
join bank.distinct_users as d2
on d1.Active_id = d2.Active_id 
and d2.Activity_month_number = d1.Activity_month_number + 1 
group by d1.Activity_year, d1.Activity_month_number
order by d1.Activity_year, d1.Activity_month_number;

-- Create a view to store the results of previous query
drop view if exists bank.retained_customers;
create view bank.retained_customers as 
select 
   d1.Activity_year,
   d1.Activity_month,
   count(distinct d1.Active_id) as Retained_customers
   from bank.distinct_users as d1
join bank.distinct_users as d2
on d1.Active_id = d2.Active_id 
and d2.Activity_month_number = d1.Activity_month_number + 1 
group by d1.Activity_year, d1.Activity_month_number
order by d1.Activity_year, d1.Activity_month_number;

select * from bank.retained_customers;

-- Modify the previous queries to list the customers lost last month.
-- this is mine, it is incorrect, I don't know what i did.
with user_activity as (
  select account_id, convert(date, date) as Activity_date,
  date_format(convert(date,date), '%M') as Activity_Month,
  date_format(convert(date,date), '%Y') as Activity_year,
  convert(date_format(convert(date,date), '%m'),UNSIGNED) as month_number
  from bank.trans
),
distinct_users as (
  select distinct account_id, Activity_month, Activity_year, month_number
  from user_activity
)
select d1.account_id, d2.account_id, d1.Activity_month, d1.Activity_year
from distinct_users d1
left join distinct_users d2 on d1.account_id = d2.account_id and d1.month_number = d2.month_number + 1
where d1.Activity_month = 'December' and d1.Activity_year = 1998 and d2.account_id is null;

/*
3.07 Activity 3
Use a similar approach to get total monthly transaction per account and 
the difference with the previous month.
*/
SELECT *, 
	LAG(total_amount,1) OVER (PARTITION BY account_id ORDER BY year,month) AS last_month,
	total_amount - LAG(total_amount,1) OVER (PARTITION BY account_id ORDER BY year,month) AS difference
FROM (SELECT SUM(amount) total_amount, account_id, YEAR(date) year,MONTH(date) month
	FROM trans
	GROUP BY 2,3,4) AS total_amount_account

/*
3.07 Activity 4
The odds are defined as the probability that the event will occur divided by the 
probability that the event will not occur. 
Explain how to transform the model from linear regression to logistic regression using the logistic function.
*/
CREATE DATABASE trainity_project_3;
USE trainity_project_3;


-- Case Study 1 (Job Data)


CREATE TABLE job_data(
ds DATE NOT NULL,
job_id INT NOT NULL,
actor_id INT NOT NULL,
`event` VARCHAR(30),
`language` VARCHAR(30),
time_spent INT,
org VARCHAR(10)
);

INSERT INTO job_data
(ds,job_id,actor_id,`event`,`language`,time_spent,org)
VALUES
("2020-11-30",21,1001,"skip","English",15,"A"),
("2020-11-30",22,1006,"transfer","Arabic",25,"B"),
("2020-11-29",23,1003,"decision","Persian",20,"C"),
("2020-11-28",23,1005,"transfer","Persian",22,"D"),
("2020-11-28",25,1002,"decision","Hindi",11,"B"),
("2020-11-27",11,1007,"decision","French",104,"D"),
("2020-11-26",23,1004,"skip","Persian",56,"A"),
("2020-11-25",20,1003,"transfer","Italian",45,"C"),
("2020-11-24",24,1009,"skip","Korean",94,"A"),
("2020-11-23",11,1008,"transfer","French",82,"C"),
("2020-11-22",25,1004,"decision","Hindi",44,"A"),
("2020-11-22",20,1007,"skip","Italian",79,"D"),
("2020-11-21",24,1002,"skip","Korean",48,"B"),
("2020-11-20",22,1005,"decision","Arabic",65,"D"),
("2020-11-19",21,1009,"transfer","English",109,"A"),
("2020-11-18",22,1003,"skip","Arabic",39,"C"),
("2020-11-17",25,1007,"decision","Hindi",77,"D"),
("2020-11-16",11,1005,"transfer","French",111,"D"),
("2020-11-15",11,1001,"transfer","French",94,"A"),
("2020-11-14",23,1006,"decision","Persian",57,"B"),
("2020-11-13",24,1009,"skip","Korean",81,"A"),
("2020-11-12",21,1007,"skip","English",76,"D"),
("2020-11-11",21,1003,"decision","English",59,"C"),
("2020-11-10",25,1002,"transfer","Hindi",67,"B"),
("2020-11-09",23,1005,"skip","Persian",73,"D"),
("2020-11-09",11,1009,"decision","French",56,"A"),
("2020-11-08",25,1008,"transfer","Hindi",112,"C"),
("2020-11-07",21,1001,"skip","English",82,"A"),
("2020-11-06",24,1006,"transfer","Korean",143,"B"),
("2020-11-05",22,1002,"decision","Arabic",93,"B"),
("2020-11-04",11,1007,"skip","French",47,"D"),
("2020-11-03",23,1003,"transfer","Persian",120,"C"),
("2020-11-02",25,1004,"transfer","Hindi",66,"A"),
("2020-11-01",22,1009,"decision","Arabic",37,"A"),
("2020-10-30",21,1008,"skip","English",77,"C");

SELECT * FROM job_data;


-- Number of jobs reviewed: Amount of jobs reviewed over time.
-- Your task: Calculate the number of jobs reviewed per hour per day for November 2020?


SELECT COUNT(DISTINCT job_id)/SUM(time_spent/3600)/COUNT(ds) 
AS jobs_reviewed_per_hour_per_day_in_nov_2020
FROM job_data
WHERE ds BETWEEN "2020-11-01" AND "2020-11-30";


-- B)Throughput: It is the no. of events happening per second.
-- Your task: Let’s say the above metric is called throughput. Calculate 7 day rolling average of throughput?
-- For throughput, do you prefer daily metric or 7-day rolling and why?


SELECT job_id, `date`, event_per_day, 
 AVG(event_per_day)OVER(ORDER BY `date` ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)AS 7_day_rolling_avg
 FROM
(SELECT job_id, ds AS `date`,
COUNT(DISTINCT `event`) AS event_per_day 
FROM job_data
GROUP BY `date` 
ORDER BY `date`)a;


-- C)Percentage share of each language: Share of each language for different contents.
-- Your task: Calculate the percentage share of each language in the last 30 days?


SELECT `language`,
COUNT(*) * 100.0 / (SELECT COUNT(*) 
FROM job_data ) AS percentage_share_of_language
FROM job_data
GROUP BY `language`;


-- D)Duplicate rows: Rows that have the same value present in them.
-- Your task: Let’s say you see some duplicate rows in the data. How will you display duplicates from the table?


SELECT *
FROM job_data
GROUP BY ds , job_id, actor_id, `event`, `language`, time_spent, org
HAVING COUNT(ds) >1 
AND COUNT(job_id) >1 
AND COUNT(actor_id) >1 
AND COUNT(`event`) >1 
AND COUNT(`language`) >1 
AND COUNT(time_spent) >1 
AND COUNT(org) >1;


-- Case Study 2 (Investigating metric spike)


CREATE TABLE users(
user_id INT NOT NULL,
created_at TIMESTAMP,
company_id INT,
`language` VARCHAR(30),
activated_at TIMESTAMP,
state VARCHAR (30),
PRIMARY KEY(user_id)
);

SET SESSION sql_mode='';
SET SQL_SAFE_UPDATES = 0;
SHOW VARIABLES LIKE "secure_file_priv";

SHOW GLOBAL VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 'ON';
SET GLOBAL local_infile = 1;
SET GLOBAL local_infile = true;


LOAD DATA INFILE
"D:\Table-1 users.csv"
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM users;

CREATE TABLE `events`(
user_id INT,
occurred_at TIMESTAMP,
event_type VARCHAR(30),
event_name VARCHAR(30),
location VARCHAR(30),
device VARCHAR(50),
user_type INT,
FOREIGN KEY (user_id) REFERENCES users(user_id)
);

LOAD DATA INFILE
"D:\Table-2 events.csv"
INTO TABLE `events`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM `events`;

CREATE TABLE email_events(
user_id INT,
occurred_at TIMESTAMP,
`action` VARCHAR(30),
user_type INT,
FOREIGN KEY (user_id) REFERENCES users(user_id)
);


LOAD DATA INFILE
"D:\Table-3 email_events.csv"
INTO TABLE email_events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM email_events;


SELECT DISTINCT(state) FROM users;
SELECT * FROM `events`;
SELECT * FROM email_events;


-- A) User Engagement: To measure the activeness of a user. Measuring if the user finds quality in a product/service.
-- Your task: Calculate the weekly user engagement?

SELECT COUNT(DISTINCT user_id) AS active_users,
WEEK(occurred_at) AS `week`
FROM `events`
WHERE event_type= 'engagement'
GROUP BY `week`;


-- B) User Growth: Amount of users growing over time for a product.
-- Your task: Calculate the user growth for product?

SELECT COUNT(user_id) AS increase_of_user,
YEARWEEK(created_at) AS week_of_year
FROM users
GROUP BY week_of_year;


-- C) Weekly Retention: Users getting retained weekly after signing-up for a product.
-- Your task: Calculate the weekly retention of users-sign up cohort?

SELECT COUNT(DISTINCT(users.user_id)) AS retained_users,
YEARWEEK(users.created_at) AS `week`
FROM `events`
INNER JOIN users ON
users.user_id=`events`.user_id
WHERE event_type='signup_flow'
GROUP BY `week`;


-- D) Weekly Engagement: To measure the activeness of a user. 
-- Measuring if the user finds quality in a product/service weekly.
-- Your task: Calculate the weekly engagement per device?

SELECT COUNT(user_id) AS no_of_users,
YEAR(occurred_at) AS `year`,
WEEK(occurred_at) AS `week`,
device
FROM `events`
WHERE `events`.event_type= 'engagement'
GROUP BY `year`, `week`, device
ORDER BY `year`, `week`, device;


-- E) Email Engagement: Users engaging with the email service.
-- Your task: Calculate the email engagement metrics?

SELECT `action`,
YEARWEEK(occurred_at) AS `week`,
COUNT(distinct user_id) AS users_engaging
FROM email_events 
GROUP BY `action`, `week` 
ORDER BY `action`, `week`;

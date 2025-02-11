CREATE database project3;
use project3;
CREATE TABLE job_data (
ds DATE,
job_id INT NOT NULL,
actor_id INT NOT NULL,
event VARCHAR(10) NOT NULL,
language VARCHAR(10) NOT NULL,
time_spent INT NOT NULL,
org CHAR(2)
);
select * from job_data;
insert into job_data (ds,job_id,actor_id,event,language,time_spent,org)
values(
'2020-11-30',21,1001,'skip','English',15,'A'),
('2020-11-30',22,1006,'transfer','Arabic',25,'B'),
('2020-11-29',23,1003,'decision','Persian',20,'C'),
('2020-11-28',23,1005,'transfer','Persian',22,'D'),
('2020-11-28',25,1002,'decision','Hindi',11,'B'),
('2020-11-27',11,1007,'decision','French',104,'D'),
('2020-11-26',23,1004,'skip','Persian',56,'A'),
('2020-11-25',20,1003,'transfer','Italian',45,'C');

# CASE STUDY 1---TASK A 
   # jobs reviewed per hour for each day in nov 2020
   SELECT ds AS DATE,
      COUNT(job_id) AS Joint_Job_Id,
      ROUND((SUM(time_spent)/3600),2) AS Time_sp_hr,
      ROUND((COUNT(job_id)/(SUM(time_spent)/3600)),2) AS job_review_phr_pday
      FROM job_data WHERE ds BETWEEN '2020-11-01' AND '2020-11-30' 
      GROUP BY ds ORDER BY ds;
      
# CASE STUDY 1---TASK B
    # calculate 7 day rolling average 
    SELECT ROUND(COUNT(event)/SUM(time_spent),2) AS weekly_avg_throughput
    FROM job_data;
    SELECT ds AS dates, ROUND(COUNT(event)/SUM(time_spent),2) AS daily_avg_throughput
    FROM job_data
    GROUP BY ds
    order by ds;
    
#CASE STUDY 1 --- TASK C
    #percentage share of each language in last 30 days
    SELECT language, ROUND(100*COUNT(*)/total,2) AS percentage,
    jd.total
    FROM job_data 
    CROSS JOIN (SELECT COUNT(*) AS total FROM job_data) AS jd
    GROUP BY language, jd.total;

#CASE STUDY 1----TASK D
    #identify duplicate rows from the job_data table
    SELECT actor_id,COUNT(*) AS duplicate
    FROM job_data
    GROUP BY actor_id
    HAVING COUNT(*) > 1;
    
    
    #CASE STUDY 2
    use project3;
    # users table
    create table users(
    user_id int,
    created_at varchar(100),
    company_id int,
    language varchar(50),
    activated_at varchar(100),
    state varchar(50));
    
    #upload users data
    
    SHOW VARIABLES LIKE 'SECURE_FILE_PRIV';
    LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
    INTO TABLE users
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    
    SELECT * FROM users;
    ALTER TABLE users add column temp_created_at datetime;
    SET SQL_SAFE_UPDATES =0;
    UPDATE users SET temp_created_at = str_to_date(created_at,'%d-%m-%Y %H:%i');
    SET SQL_SAFE_UPDATES = 1;
    alter table users DROP COLUMN created_at;
    ALTER TABLE users CHANGE COLUMN temp_created_at created_at DATETIME;
     
     # events table
     CREATE TABLE events(
     user_id INT,
     occurred_at VARCHAR(100),
     event_type VARCHAR(50),
     event_name VARCHAR(100),
     loacation VARCHAR(50),
     device VARCHAR(100),
     user_type INT);
    # use project3;
    # drop table events;
     LOAD DATA INFILE  "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
     INTO TABLE events
     FIELDS TERMINATED BY ','
     ENCLOSED BY '"'
     LINES TERMINATED BY '\n'
     IGNORE 1 ROWS;
     SELECT * FROM events;
     desc events;
     
     SELECT * FROM events;
     
    ALTER TABLE events add column temp_occurred_at datetime;
    SET SQL_SAFE_UPDATES =0;
    UPDATE events SET temp_occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');

    SET SQL_SAFE_UPDATES = 1;
    alter table events DROP COLUMN occurred_at;
    ALTER TABLE events CHANGE COLUMN temp_occurred_at occurred_at DATETIME;
   #drop table events;
   
   
    # email-events table
     CREATE TABLE email_events(
     user_id INT,
     occurred_at VARCHAR(100),
     action_type VARCHAR(100),
     user_type INT);
     
    
     LOAD DATA INFILE  "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
     INTO TABLE email_events
     FIELDS TERMINATED BY ','
     ENCLOSED BY '"'
     LINES TERMINATED BY '\n'
     IGNORE 1 ROWS;
     SELECT * FROM email_events;
     desc email_events;
     
#CASE STUDY 2 ---TASK A
    #weekly user engagement 
    SELECT EXTRACT(WEEK FROM occurred_at) AS week_num,
    COUNT(DISTINCT user_id) AS active_users
    FROM events WHERE event_type = 'engagement' AND occurred_at IS NOT NULL
    GROUP BY week_num
    ORDER BY week_num;
    
    #CASE STUDY 2----TASK B
     SELECT COUNT(*) FROM users WHERE state = 'active';
     SELECT DISTINCT state FROM users;
     WITH weekly_active_users AS (
     SELECT EXTRACT(YEAR FROM created_at) AS year,
      EXTRACT(WEEK FROM created_at) AS week_number,
     COUNT(DISTINCT user_id) AS num_of_users
     FROM users
     GROUP BY year,week_number)
     SELECT year,week_number,num_of_users,
     SUM(num_of_users) OVER (ORDER BY year,week_number) AS cumulative_users
     FROM weekly_active_users
     ORDER BY year,week_number;
     
 #CASE STUDY 2----TASK C    
    #weekly retention analysis
    SELECT first AS "week_numbers",
    SUM(CASE WHEN week_number=0 THEN 1 ELSE 0 END) AS "week_0",
    SUM(CASE WHEN week_number=1 THEN 1 ELSE 0 END) AS "week_1",
    SUM(CASE WHEN week_number=2 THEN 1 ELSE 0 END) AS "week_2",
    SUM(CASE WHEN week_number=3 THEN 1 ELSE 0 END) AS "week_3",
    SUM(CASE WHEN week_number=4 THEN 1 ELSE 0 END) AS "week_4",
    SUM(CASE WHEN week_number=5 THEN 1 ELSE 0 END) AS "week_5",
    SUM(CASE WHEN week_number=6 THEN 1 ELSE 0 END) AS "week_6",
    SUM(CASE WHEN week_number=7 THEN 1 ELSE 0 END) AS "week_7",
    SUM(CASE WHEN week_number=8 THEN 1 ELSE 0 END) AS "week_8",
    SUM(CASE WHEN week_number=9 THEN 1 ELSE 0 END) AS "week_9",
    SUM(CASE WHEN week_number=10 THEN 1 ELSE 0 END) AS "week_10",
    SUM(CASE WHEN week_number=11 THEN 1 ELSE 0 END) AS "week_11",
    SUM(CASE WHEN week_number=12 THEN 1 ELSE 0 END) AS "week_12",
    SUM(CASE WHEN week_number=13 THEN 1 ELSE 0 END) AS "week_13",
    SUM(CASE WHEN week_number=14 THEN 1 ELSE 0 END) AS "week_14",
    SUM(CASE WHEN week_number=15 THEN 1 ELSE 0 END) AS "week_15",
    SUM(CASE WHEN week_number=16 THEN 1 ELSE 0 END) AS "week_16",
    SUM(CASE WHEN week_number=17 THEN 1 ELSE 0 END) AS "week_17",
    SUM(CASE WHEN week_number=18 THEN 1 ELSE 0 END) AS "week_18"
    FROM ( SELECT m.user_id,m.login_week,n.first,m.login_week-n.first AS week_nuumber
    FROM ( SELECT user_id,EXTRACT(WEEK FROM occurred_at) AS login_week
    FROM events
    GROUP BY user_id,login_week)m
    JOIN (SELECT user_id,MIN(EXTRACT(WEEK FROM occurred_at)) AS first
    FROM events
    GROUP BY user_id)n
    ON m.user_id=n.user_id
    )sub
    GROUP BY first
    ORDER BY first;
    
    #CASE STUDY 2---------TASK D
    # weekly engagement per device
    use project3;
    SELECT
       EXTRACT(WEEK FROM occurred_at) AS week_number,
       COUNT(DISTINCT CASE WHEN device = 'dell inspiron notebook' THEN user_id ELSE NULL END) AS dell_inspiron_notebook,
	   COUNT(DISTINCT CASE WHEN device = 'iphone 5' THEN user_id ELSE NULL END) AS iphone_5,
     COUNT(DISTINCT CASE WHEN device = 'iphone 4s' THEN user_id ELSE NULL END) AS iphone_4s,
     COUNT(DISTINCT CASE WHEN device = 'iphone 5s' THEN user_id ELSE NULL END) AS iphone_5s,
     COUNT(DISTINCT CASE WHEN device = 'ipad air' THEN user_id ELSE NULL END) AS ipad_air,
     COUNT(DISTINCT CASE WHEN device = 'windows surface' THEN user_id ELSE NULL END) AS windows_surface,
     COUNT(DISTINCT CASE WHEN device = 'macbook air' THEN user_id ELSE NULL END) AS macbook_air,
     COUNT(DISTINCT CASE WHEN device = 'macbook pro' THEN user_id ELSE NULL END) AS macbook_pro,
     COUNT(DISTINCT CASE WHEN device = 'ipad mini' THEN user_id ELSE NULL END) AS ipad_mini,
     COUNT(DISTINCT CASE WHEN device = 'kindle fire' THEN user_id ELSE NULL END) AS kindle_fire,
     COUNT(DISTINCT CASE WHEN device = 'amazon fire phone' THEN user_id ELSE NULL END) AS amazon_fire_phone,
     COUNT(DISTINCT CASE WHEN device = 'nexus 5' THEN user_id ELSE NULL END) AS nexus_5,
     COUNT(DISTINCT CASE WHEN device = 'nexus 7' THEN user_id ELSE NULL END) AS nexus_7,
     COUNT(DISTINCT CASE WHEN device = 'nexus 10' THEN user_id ELSE NULL END) AS nexus_10,
     COUNT(DISTINCT CASE WHEN device = 'samsung galaxy s4' THEN user_id ELSE NULL END) AS samsung_galaxy_s4,
     COUNT(DISTINCT CASE WHEN device = 'samsung galaxy tablet' THEN user_id ELSE NULL END) AS samsung_galaxy_tablet,
     COUNT(DISTINCT CASE WHEN device = 'samsung galaxy note' THEN user_id ELSE NULL END) AS samsung_galaxy_note,
     COUNT(DISTINCT CASE WHEN device = 'lenovo thinkpad' THEN user_id ELSE NULL END) AS lenovo_thinkpad,
     COUNT(DISTINCT CASE WHEN device = 'acer aspire notebook' THEN user_id ELSE NULL END) AS acer_aspire_notebook,
     COUNT(DISTINCT CASE WHEN device = 'asus chromebook' THEN user_id ELSE NULL END) AS asus_chromebook,
     COUNT(DISTINCT CASE WHEN device = 'htc one' THEN user_id ELSE NULL END) AS htc_one,
     COUNT(DISTINCT CASE WHEN device = 'nokia lumia 635' THEN user_id ELSE NULL END) AS nokia_lumia635,
     COUNT(DISTINCT CASE WHEN device = 'mac mini' THEN user_id ELSE NULL END) AS mac_mini,
     COUNT(DISTINCT CASE WHEN device = 'hp pavilion desktop' THEN user_id ELSE NULL END) AS hp_pavilion_desktop,
     COUNT(DISTINCT CASE WHEN device = 'dell inspiron desktop' THEN user_id ELSE NULL END) AS dell_inspiron_desktop
     FROM events WHERE event_type = 'engagement'
     GROUP BY week_number
     ORDER BY week_number;
     
     #PART 2 -------TASK E
     #email engagement analysis
     SELECT
    100 * SUM(CASE WHEN email_action = 'email_open' THEN 1 ELSE 0 END) /
    SUM(CASE WHEN email_action = 'email_sent' THEN 1 ELSE 0 END) AS email_open_rate,
    100 * SUM(CASE WHEN email_action = 'email_clicked' THEN 1 ELSE 0 END) /
    SUM(CASE WHEN email_action = 'email_sent' THEN 1 ELSE 0 END) AS email_clicked_rate
FROM (
    SELECT *,
        CASE
            WHEN action IN ('sent_weekly_digest', 'sent_reengagement_email') THEN 'email_sent'
            WHEN action = 'email_open' THEN 'email_open'
            WHEN action = 'email_clickthrough' THEN 'email_clicked'
            ELSE NULL
        END AS email_action
    FROM project3.email_events
) a;

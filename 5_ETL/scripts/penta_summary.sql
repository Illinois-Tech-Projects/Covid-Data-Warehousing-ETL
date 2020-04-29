
-- sequence  by states
DROP TABLE IF EXISTS `nytimes_covid_states_day_seq`;
CREATE TABLE `nytimes_covid_states_day_seq`
AS
SELECT *, RANK() OVER( PARTITION BY `state` ORDER BY `date`) AS seq
FROM `nytimes_covid_states`;



-- us_new_cases summary if null should replace with the first day total us cases
CREATE OR REPLACE VIEW us_new_cases_summary as
SELECT DISTINCT `date`, 
IFNULL(sum(new_cases) over (PARTITION BY date), 
(SELECT sum(`cases`) FROM nytimes_covid_states_day_seq where seq=1) ) AS us_new_cases, 
'US' AS city
FROM nytime_daily_state_newcases;
SELECT * FROM us_new_cases_summary;



-- Todays cases  by states summary
DROP TABLE IF EXISTS `nytimes_covid_us_to_date`;
CREATE TABLE `nytimes_covid_us_to_date`
AS
SELECT 
a.state, 
a.`date`, 
a.cases
FROM itmd526.nytimes_covid_states_day_seq a
JOIN (
select state, max(seq) as max_seq 
from `nytimes_covid_states_day_seq`
GROUP BY state) b
WHERE a.seq = b.max_seq AND a.state = b.state;
-- Query 1: Monthly Active Users by Region
SET hive.cli.print.header=true;
SELECT dim_users.region, COUNT(DISTINCT fact_user_actions.user_id) AS active_users
FROM fact_user_actions
JOIN dim_users ON fact_user_actions.user_id = dim_users.user_id
WHERE fact_user_actions.year = 2023 AND fact_user_actions.month = 9
GROUP BY dim_users.region;

-- Query 2: Top Categories by Play Count
SELECT dim_content.category, COUNT(*) AS play_count
FROM fact_user_actions
JOIN dim_content ON fact_user_actions.content_id = dim_content.content_id
WHERE fact_user_actions.action = 'play'
GROUP BY dim_content.category
ORDER BY play_count DESC
LIMIT 5;

-- Query 3: Average Session Count Per Week
SELECT fact_user_actions.year, WEEKOFYEAR(fact_user_actions.event_timestamp) AS week, 
       COUNT(DISTINCT fact_user_actions.session_id) AS total_sessions
FROM fact_user_actions
GROUP BY fact_user_actions.year, WEEKOFYEAR(fact_user_actions.event_timestamp)
ORDER BY fact_user_actions.year, week;


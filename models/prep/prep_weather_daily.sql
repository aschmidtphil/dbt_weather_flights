WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *
		, DATE_PART('day', date)::INT AS date_day
		, DATE_PART('month', date) AS date_month
		, DATE_PART('year', date) AS date_year
		, DATE_PART('week', date) AS cw
		, TO_CHAR (date, 'Month') AS month_name
		, TO_CHAR (date, 'Day') AS weekday
    FROM daily_data 
),
add_more_features AS (
    SELECT *
		, (CASE 
			WHEN month_name IN ('Dezember, January, February') THEN 'winter'
			WHEN month_name IN ('March','April','May') THEN 'spring'
            WHEN month_name IN ('June', 'July', 'August') THEN 'summer'
            WHEN month_name IN ('September', 'October', 'November') THEN 'autumn'
		END) AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date
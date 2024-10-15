WITH routes AS 
(	SELECT 
        CONCAT(origin, ' - ', dest) AS Route,
        origin,                       
        dest, 
      --  flight_date,
      --  dep_time,
        COUNT(*) AS total_flights,    
        COUNT(DISTINCT tail_number) AS unique_airplanes, 
        COUNT(DISTINCT airline) AS unique_airlines,      
        ROUND(AVG(actual_elapsed_time),2) AS avg_elapsed_time,    
        ROUND(AVG(arr_delay),2) AS avg_arrival_delay,             
        MAX(arr_delay) AS max_delay,                     
        MIN(arr_delay) AS min_delay,                    
        SUM(cancelled) AS total_cancelled,              
        SUM(diverted) AS total_diverted                 
    FROM {{ref('prep_flights')}}
    GROUP BY origin, dest --,flight_date, dep_time
), 
locations AS
(	SELECT
       	origin,
		dest,
		tail_number,
		airline,
		actual_elapsed_time,
		arr_delay,
		cancelled,
		diverted,
		origin_airport.city AS origin_city,
		origin_airport.country AS origin_country,
		origin_airport.name AS origin_name,
		dest_airport.city AS dest_city,
		dest_airport.country AS dest_country,
		dest_airport.name AS dest_name
	FROM {{ref('prep_flights')}}
	LEFT JOIN {{ref('prep_airports')}} AS origin_airport      -- Alias for the origin airport
    ON origin_airport.faa = prep_flights.origin
	LEFT JOIN {{ref('prep_airports')}} AS dest_airport        -- Alias for the destination airport
    ON dest_airport.faa = prep_flights.dest
	)
SELECT 	route,
        --flight_date,
        --dep_time,
		locations.origin,
		origin_city,
		origin_country,
		origin_name,
		locations.dest,
		dest_city,
		dest_country,
		dest_name,
		total_flights,    
        unique_airplanes, 
        unique_airlines,      
        avg_elapsed_time,    
        avg_arrival_delay,             
        max_delay,                     
        min_delay,                    
        total_cancelled,              
        total_diverted
FROM locations 
JOIN routes
ON locations.origin = routes.origin
AND locations.dest = routes.dest
GROUP BY 
    routes.route,
    routes.origin,
    locations.origin,
    locations.dest,
    locations.origin_city,
    locations.origin_country,
    locations.origin_name,
    routes.dest,
    locations.dest_city,
    locations.dest_country,
    locations.dest_name,
    routes.total_flights,    
    routes.unique_airplanes, 
    routes.unique_airlines,      
    routes.avg_elapsed_time,    
    routes.avg_arrival_delay,             
    routes.max_delay,                     
    routes.min_delay,                    
    routes.total_cancelled,              
    routes.total_diverted
ORDER BY total_flights desc
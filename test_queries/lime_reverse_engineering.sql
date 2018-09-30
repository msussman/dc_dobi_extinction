SELECT DISTINCT
loc.date,
COUNT(distinct bike.bike_id) as daily_bike_count,
COUNT(distinct scooter.bike_id) as daily_scooter_count,
COUNT(distinct lime.bike_id) as daily_lime_count,
COUNT(distinct lime.bike_id) -
COUNT(distinct bike.bike_id) -
COUNT(distinct scooter.bike_id)  as daily_unknown_count
FROM
(select distinct
	created::date as date, 
	bike_id 
	from bike_locations
	where provider='limebike') as loc
LEFT JOIN
((SELECT DISTINCT
	bike_id
	FROM
	(SELECT DISTINCT 
	bike_id,
	min(created::date) as start_date
	FROM bike_locations
	WHERE
	provider='limebike'
	group by bike_id) as lime_starts
	/*any limes that started before 3/10 assumed to be bikes*/
	WHERE start_date < '2018-03-10')
	UNION
	(SELECT DISTINCT 
	bike_id
	FROM bike_locations
	WHERE
	provider='limebike' AND
	raw->'attributes'->>'vehicle_type'='bike'
	group by 1)) as bike
on loc.bike_id = bike.bike_id 
LEFT JOIN
(SELECT DISTINCT 
	bike_id
	FROM bike_locations
	WHERE
	provider='limebike' AND
	raw->'attributes'->>'vehicle_type'='scooter'
	group by 1) as scooter
on loc.bike_id = scooter.bike_id 
LEFT JOIN
(SELECT DISTINCT 
	bike_id
	FROM bike_locations
	WHERE
	provider='limebike'
	group by 1) as lime
on loc.bike_id = lime.bike_id 
group by 1
order by 1;
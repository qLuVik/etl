SELECT
    pagename,
    date_part('hour', datetime) AS hour,
    ROUND(AVG(pageviewcount), 2) AS avg_pageviews
FROM
    pageview_counts
WHERE
    pagename IN ('Яндекс', 'ВКонтакте', 'Mail.ru')
GROUP BY
    pagename,
    date_part('hour', datetime)
ORDER BY
    pagename,
    hour;


select *
from public.person p 


select avg(age) as avg_age, min(age) as min_age, max(age) as max_age
from public.person p ;

SELECT 
    CASE 
        WHEN address LIKE 'г. %' THEN SUBSTRING(address FROM 4 FOR POSITION(',' IN address) - 4)
        WHEN address LIKE 'д. %' THEN SUBSTRING(address FROM 4 FOR POSITION(',' IN address) - 4)
        WHEN address LIKE 'с. %' THEN SUBSTRING(address FROM 4 FOR POSITION(',' IN address) - 4)
        WHEN address LIKE 'п. %' THEN SUBSTRING(address FROM 4 FOR POSITION(',' IN address) - 4)
        WHEN address LIKE 'к. %' THEN SUBSTRING(address FROM 4 FOR POSITION(',' IN address) - 4)
        WHEN address LIKE 'клх %' THEN SUBSTRING(address FROM 6 FOR POSITION(',' IN address) - 6)
        WHEN address LIKE 'ст. %' THEN SUBSTRING(address FROM 5 FOR POSITION(',' IN address) - 5)
    END AS city,
    COUNT(*) AS population
FROM person
WHERE deleted_at IS NULL
GROUP BY city
ORDER BY population DESC
LIMIT 5;

SELECT 
    EXTRACT(YEAR FROM registration_date) AS year,
    EXTRACT(MONTH FROM registration_date) AS month,
    COUNT(*) AS registrations_count
FROM person
WHERE registration_date >= CURRENT_DATE - INTERVAL '1 year'
    AND deleted_at IS NULL
GROUP BY year, month
ORDER BY year, month;
		
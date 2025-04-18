SELECT x.pagename, x.hr AS "hour", x.average AS "average pageviews"
FROM (
SELECT
pagename,
date_part('hour', datetime) AS hr,
AVG(pageviewcount) AS average,
ROW_NUMBER() OVER (PARTITION BY pagename ORDER BY AVG(pageviewcount) DESC) AS row_number
FROM pageview_counts
WHERE pagename IN ("Яндекс", "ВКонтакте", "Mail.ru")
GROUP BY pagename, hr
) AS x
WHERE x.row_number = 1;
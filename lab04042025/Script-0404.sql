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
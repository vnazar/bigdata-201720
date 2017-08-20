SELECT extract(HOUR FROM timestamp_ms) AS hour, extract(DOW FROM timestamp_ms) AS dow, count(*) AS Total FROM dump01
WHERE timestamp_ms NOTNULL
GROUP BY hour, dow
ORDER BY Total DESC;


CREATE INDEX timestamp_ms ON dump01 ("timestamp_ms");
DROP INDEX timestamp_ms;

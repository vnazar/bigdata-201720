SELECT "place.country" AS country, count(*) AS Total FROM dump01
WHERE "place.country" NOTNULL
  AND "coordinates.coordinates.x" > -10.195312 AND "coordinates.coordinates.x" < 31.113281
  AND "coordinates.coordinates.y" > 36.031332 AND "coordinates.coordinates.y" < 62.021528
GROUP BY "place.country"
ORDER BY Total DESC;


CREATE INDEX coordinates ON dump01 ("coordinates.coordinates.x", "coordinates.coordinates.y");
DROP INDEX coordinates;

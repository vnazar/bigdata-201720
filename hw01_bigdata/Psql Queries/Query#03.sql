SELECT "retweeted_status.user.name", "retweeted_status.user.id" AS id, count(*) FROM dump01
WHERE "retweeted_status.user.name" NOTNULL
  AND "retweeted_status.user.id" NOTNULL
  AND "retweeted_status.retweet_count" > 1000
GROUP BY "retweeted_status.user.id", "retweeted_status.user.name"
ORDER BY count DESC;


CREATE INDEX retweet_count ON dump01 ("retweeted_status.retweet_count");
DROP INDEX retweet_count;

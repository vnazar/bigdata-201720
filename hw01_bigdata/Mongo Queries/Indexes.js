// Index for Query#01
db.north_korea_tweets.createIndex({
  "timestamp_ms": -1
});

// Index for Query#02
db.north_korea_tweets.createIndex({
  "coordinates.coordinates.0": -1,
  "coordinates.coordinates.1": -1
});


//Index for Query#03
db.north_korea_tweets.createIndex({
  "retweeted_status.retweet_count": -1
});

db.north_korea_tweets.aggregate([{
    $match: {
      timestamp_ms: {
        $exists: true
      }
    }
  },
  {
    $project: {
      "h": {
        "$hour": "$timestamp_ms"
      },
      "d": {
        "$dayOfWeek": "$timestamp_ms"
      }
    }
  },
  {
    $group: {
      _id: {
        hour: "$h",
        day: "$d"
      },
      count: {
        $sum: 1
      }
    }
  }
]);

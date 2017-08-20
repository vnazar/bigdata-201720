db.north_korea_tweets.aggregate([{
    $match: {
      "retweeted_status.user.id": {
        $exists: true,
        $ne: null
      },
      "retweeted_status.retweet_count": {
        $gt: 10000
      }
    }
  },
  {
    $group: {
      _id: {
        id: "$retweeted_status.user.id"
      },
      name: {
        $first: "$retweeted_status.user.name"
      },
      count: {
        $sum: 1
      }
    }

  },
  {
    $sort: {
      count: -1
    }
  }
])

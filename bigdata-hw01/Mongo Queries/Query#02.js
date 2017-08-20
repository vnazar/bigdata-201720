db.north_korea_tweets.aggregate([{
    $match: {
      "place.country": {
        $exists: true,
        $ne: null
      },
      "coordinates.coordinates.0": {
        $gt: -10.195312,
        $lt: 31.113281
      },
      "coordinates.coordinates.1": {
        $gt: 36.031332,
        $lt: 62.021528
      }
    }
  },
  {
    $group: {
      _id: {
        location: "$place.country"
      },
      count: {
        $sum: 1
      },
    }
  },
  {
    $sort: {
      count: -1
    }
  }
]);

db.north_korea_tweets.createIndex({
  "coordinates.coordinates.0": -1,
  "coordinates.coordinates.1": -1
})

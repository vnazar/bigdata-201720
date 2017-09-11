db.reviews.aggregate([{
    $lookup: {
      from: 'business',
      localField: 'business_id',
      foreignField: 'business_id',
      as: 'business'
    }
  },
  {
    $unwind: "$business"
  },
  {
    $limit: 500000
  },
  {
    $match: {
      "business.categories": {
        $in: ["Bikes"]
      }
    }
  },
  {
    $group: {
      _id: {
        neighborhood: "$business.neighborhood"
      },
      count: {
        $sum: 1
      }
    }
  }
])

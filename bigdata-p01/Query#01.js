db.reviews.aggregate([{
    $lookup: {
      from: 'business',
      localField: 'business_id',
      foreignField: 'business_id',
      as: 'business'
    }
  },
  {
    $group: {
      _id: '$business.state',
      average: {
        $avg: "$stars"
      }
    }
  }
])

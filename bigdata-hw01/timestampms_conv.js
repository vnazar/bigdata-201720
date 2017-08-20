db.north_korea_tweets.find({timestamp_ms: {$exists:true}}).forEach( function(x) {
    db.north_korea_tweets.update({_id: x._id},
    {
        $set: {timestamp_ms: new Date(parseInt(x.timestamp_ms))}
    });
});

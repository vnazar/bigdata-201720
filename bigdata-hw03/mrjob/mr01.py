from __future__ import division

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import time
import csv


class UsersCount(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper_input(self, _, line):
        if 'name' in line:
            yield line['business_id'], ('business', line['name'], line['business_id'], line['categories'], line['latitude'], line['longitude'], line['state'])
        if 'user_id' in line:
            votes = line['useful'] + line['funny'] + line['cool']

            yield line['business_id'], ('reviews', line['review_id'], line['stars'], line['date'], votes)

    def reducer_01(self, _, value):
        business = dict()
        reviews = []
        for v in value:
            if v[0] == 'business':
                _, business_name, business_id, categories, latitude, longitude, state = v
                business['categories'] = categories
                business['name'] = business_name
                business['id'] = business_id
                business['categories'] = categories
                business['latitude'] = latitude
                business['longitude'] = longitude
                business['state'] = state
            if v[0] == 'reviews':
                _, review_id, stars, date, votes = v
                reviews.append((review_id, stars, date, votes))


        if any(business) and reviews:
            for review_id, stars, date, votes in reviews:
                for category in business['categories']:
                    writer.writerow((review_id,
                                     category,
                                     business['id'].encode("utf-8"),
                                     business['name'].encode("utf-8"),
                                     business['latitude'],
                                     business['longitude'],
                                     date,
                                     stars,
                                     business['state'].encode("utf-8"),
                                     votes
                                     ))
                    # yield (review_id, g), (business_lat, business_long, stars)

    def steps(self):
        return [MRStep(
            mapper=self.mapper_input,
            reducer=self.reducer_01
        )
        ]


if __name__ == '__main__':
    f = open('result_01_30000.csv', 'wb')
    writer = csv.writer(f)
    writer.writerow(('review_id', 'category', 'business_id', 'business_name', 'latitude', 'longitude', 'date', 'stars', 'state', 'votes'))
    start_time = time.time()
    UsersCount.run()
    print 'Time lapsed: {} seconds.'.format(time.time() - start_time)
    f.close()

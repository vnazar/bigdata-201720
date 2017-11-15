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
            yield line['business_id'], ('business', line['categories'])
        if 'user_id' in line:
            yield line['business_id'], ('reviews', line['stars'])

    def reducer_01(self, _, value):
        genres = []
        reviews = []
        for v in value:
            if v[0] == 'business':
                genres = v[1]
            if v[0] == 'reviews':
                reviews.append(v[1])

        if genres and reviews:
            for g in genres:
                for r in reviews:
                    yield (g, r), 1

    def reducer_02(self, genre_star, val):
        writer.writerow((genre_star[0], genre_star[1], sum(val)))

    def steps(self):
        return [MRStep(
            mapper=self.mapper_input,
            reducer=self.reducer_01
        ),
            MRStep(
                reducer=self.reducer_02
            ),
            # MRStep(
            #     reducer=self.reducer_03
            # )
        ]


if __name__ == '__main__':
    f = open('result_total.csv', 'wb')
    writer = csv.writer(f)
    writer.writerow(('category', 'calification', 'total'))
    start_time = time.time()
    UsersCount.run()
    print 'Time lapsed: {} seconds.'.format(time.time() - start_time)
    f.close()

from __future__ import division

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import time


class UsersCount(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper_input(self, _, line):
        if 'name' in line:
            yield line['business_id'], ('business', line['categories'])
        if 'user_id' in line:
            yield line['business_id'], ('reviews', line['user_id'], line['useful'] + line['funny'] + line['cool'])

    def reducer_01(self, _, value):
        genres = None
        user_votes = []
        for v in value:
            if v[0] == 'business':
                genres = v[1]
            if v[0] == 'reviews':
                user_votes.append([v[1], v[2]])

        if genres and user_votes:
            for g in genres:
                for user, votes in user_votes:
                    yield (g, user), (1, votes)

    def reducer_02(self, genre_user, one_votes):
        total = 0
        votes = 0
        for one, votes in one_votes:
            total += one
            votes += votes
        yield genre_user[0], (total, float(votes) / float(total), genre_user[1])

    def reducer_03(self, genre, total_score_user):
        yield genre, max(total_score_user)

    def steps(self):
        return [MRStep(
            mapper=self.mapper_input,
            reducer=self.reducer_01
        ),
            MRStep(
                reducer=self.reducer_02
            ),
            MRStep(
                reducer=self.reducer_03
            )
        ]


if __name__ == '__main__':
    start_time = time.time()
    UsersCount.run()
    print 'Time lapsed: {} seconds.'.format(time.time() - start_time)

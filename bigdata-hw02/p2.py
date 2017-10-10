from __future__ import division

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import itertools
import time


class JaccardSimilarity(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper_input(self, _, line):
        yield line['user_id'], line['business_id']

    def reducer_01(self, user, business):
        business = list(business)
        total_business = len(business)
        for b in business:
            yield b, (user, total_business)

    def reducer_02(self, _, user_totalbusiness):
        for user_a, user_b in itertools.combinations(user_totalbusiness, 2):
            yield (user_a[0], user_b[0]), user_a[1] + user_b[1]

    def mapper_03(self, usera_userb, total_a_b):
        yield (usera_userb, total_a_b), 1

    def reducer_03(self, usera_userb_totalab, ones):
        yield 'MAX', [sum(ones), usera_userb_totalab]

    def mapper_04(self, unique_key, usera_userb_totalab):
        yield unique_key, (float(usera_userb_totalab[0]) / float(usera_userb_totalab[1][1] - usera_userb_totalab[0]), usera_userb_totalab[1][0])

    def reducer_04(self, key, jaccard_userab):
        yield key, max(jaccard_userab)

    def steps(self):
        return [MRStep(
            mapper=self.mapper_input,
            reducer=self.reducer_01
        ),
            MRStep(
                reducer=self.reducer_02
            ),
            MRStep(
                mapper=self.mapper_03,
                reducer=self.reducer_03
            ),
            MRStep(
                mapper=self.mapper_04,
                reducer=self.reducer_04
            )
        ]


if __name__ == '__main__':
    start_time = time.time()
    JaccardSimilarity.run()
    print 'Time lapsed: {} seconds.'.format(time.time() - start_time)

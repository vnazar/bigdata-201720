from __future__ import division

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import itertools
import math
import time


class CosineSimilaritySimple(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper_input(self, _, line):
        yield line['business_id'], (line['user_id'], line['stars'] / 5)

    def reducer_01(self, _, user_stars):
        for usera, userb in itertools.combinations(user_stars, 2):
            yield (usera[0], userb[0]), (usera[1], userb[1])

    def reducer_02(self, usera_userb, starsa_starsb):
        ab = 0
        aa = 0
        bb = 0

        for starsa, starsb in starsa_starsb:
            ab += starsa * starsb
            aa += starsa ** 2
            bb += starsb ** 2

        similarity = float(ab) / float((math.sqrt(aa) * math.sqrt(bb)))
        # print 'similarity| ab: {}, sqrt_aa: {}, sqrt_bb: {}'.format(ab, math.sqrt(aa), math.sqrt(bb))
        yield 'MAX', (similarity, usera_userb)

    def reducer_03(self, unique_key, similarity_usera_userb):
        yield unique_key, max(similarity_usera_userb)

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
    CosineSimilaritySimple.run()
    print 'Time lapsed: {} seconds.'.format(time.time() - start_time)

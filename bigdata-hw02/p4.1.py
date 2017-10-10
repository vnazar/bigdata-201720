from __future__ import division

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import csv
import math


class CosineSimilarityAllCombinations(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper_input(self, _, line):
        yield 'A', ('USER'+line['user_id'], 'BUSINESS'+line['business_id'], line['stars']/5)

    def reducer_01(self, business, user_stars):
        for usera, userb in itertools.combinations(user_stars, 2):
            yield (usera[0], userb[0]), ((usera[1], usera[2]), (userb[1], userb[2]))

    ### Obtengo "45672"	["11", 227] - movie_id [user_id, total]

    def reducer_02(self, usera_userb, starsa_starsb):
        ab = 0
        aa = 0
        bb = 0

        for starsa, starsb in starsa_starsb:
            if starsa[0] == starsb[0]:
                ab += starsa[1] * starsb[1]
                aa += starsa[1]**2
                bb += starsb[1]**2

        similarity = float(ab)/float((math.sqrt(aa)*math.sqrt(bb))) if ab != 0 else 0
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
    CosineSimilarityAllCombinations.run()
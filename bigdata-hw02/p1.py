from __future__ import division

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import time


class UniqueReview(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper_input(self, _, line):
        """
        
        :param _: -
        :param line: -
        :return: (word, review_id, length_of_text), 1
        """
        text = line['text'].replace('.', '').replace(',', '').replace("\n", ' ').split(' ')
        for t in text:
            yield (t, line['review_id'], len(text)), 1

    def reducer_01(self, word_review_length, value):
        """ Here I add all 1's generated for the specific key to find the repeated words.
        
        :param word_review_length: Tuple with three elements, word, review_id and text length.
        :param value: Generators withs 1's.
        :return: (review_id, length_of_text), sum_of_ones
        """
        yield (word_review_length[1], word_review_length[2]), sum(value)

    def reducer_02(self, review_length, value):
        """ Here I define a single key for all reviews for find in the next step the maximum unique word.

        :param review_length: Tuple with two elements, review_id and text lenght.
        :param value: sum_of_ones
        :return: 'MAX', (length, review_id)
        """
        length = len(list(value))
        if length == review_length[1]:
            yield 'MAX', (review_length[1], review_length[0])

    def reducer_03(self, unique_key, length_review):
        """

        :param unique_key: MAX
        :param length_review: Tuple with text length and review_id
        :return: Return the review with the maximum length and his review_id.
        """
        yield unique_key, max(length_review)

    def steps(self):
        return [MRStep(
            mapper=self.mapper_input,
            reducer=self.reducer_01
        ),

            MRStep(
                reducer=self.reducer_02,
            ),
            MRStep(
                reducer=self.reducer_03
            )
        ]


if __name__ == '__main__':
    start_time = time.time()
    UniqueReview.run()
    print 'Time lapsed: {} seconds.'.format(time.time() - start_time)

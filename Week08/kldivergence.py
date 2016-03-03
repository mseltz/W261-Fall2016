from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import numpy as np

class kldivergence(MRJob):
    
    def mapper1(self, _, line):
        index = int(line.split('.',1)[0])
        letter_list = re.sub(r"[^A-Za-z]+", '', line).lower()
        count = {}
        for l in letter_list:
            if count.has_key(l):
                count[l] += 1
            else:
                count[l] = 1
        for key in count:
            # without smoothing
            #yield key, [index, (count[key]) * 1.0 / (len(letter_list))]
            
            # with smoothing
            yield key, [index, (count[key] + 1) * 1.0 / (len(letter_list) + 24)]


    def reducer1(self, key, values):
        postings = {1:None, 2:None}
        for val in values:
            postings[val[0]] = val[1]
        sim = np.log(postings[1]/postings[2]) * postings[1]
        yield None, sim
    
    def reducer2(self, key, values):
        kl_sum = 0
        for value in values:
            kl_sum = kl_sum + value
        yield None, kl_sum
            
    def steps(self):
        return [MRStep(mapper=self.mapper1,
                       reducer=self.reducer1),
                MRStep(reducer=self.reducer2)]

if __name__ == '__main__':
    kldivergence.run()
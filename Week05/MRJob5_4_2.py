from mrjob.job import MRJob
from mrjob.step import MRStep
import math
from itertools import combinations, product, chain

class similarity(MRJob):
    
    """
    Specify some custom options so we only have to write one MRJob class for each part
    """
    def configure_options(self):
        super(similarity, self).configure_options()
        self.add_passthrough_option('--method', default='jaccard')
    
    """
    Build the full co-occurrence matrix
    - For each partial stripe, emit the stripe and the inverse stripe
    - Example: (dog, {cat:2, bird:3})
       - Emit original (dog, {cat:2, bird:3})
       - Emit inverse (cat, {dog:2}), (bird, {dog:3})
    """
    
    def mapper_buildFullMatrix(self, _, line):
        fields = line.strip().split('\t')
        word1 = eval(fields[0])
        stripe = eval(fields[1])
        yield word1, stripe
        for word2 in stripe:
            yield word2, {word1: stripe[word2]}
        
    def reducer_buildFullMatrix(self, key, values):
        stripe = {}
        for val in values:
            for word in val:
                if word in stripe:
                    stripe[word] += val[word]
                else:
                    stripe[word] = val[word]
        yield key, stripe
    
    """
    Jaccard similarity
    - We need to only create the pairs we know we want to emit
    - We only emit anything if one item in the pair has a value
    - We binarize on whether the co-occurrence support is in the list of values
    - There will be no values of zero in the list of values
    - bin1 = true/false if word1 is > 0 (if word1 is in set, bin1=1)
    - Emit (word1, word2), (bin1 and bin2, bin1, bin2)
    """
    
    def mapper_Jaccard_init(self):
        self.vocab = set()
        with open('basisWords.txt','r') as myfile:
            for word in myfile:
                self.vocab.add(word.strip())
        
    def mapper_Jaccard(self, key, value):
        valueSet = set(value.keys())
        allPairs = chain(product(valueSet, self.vocab.difference(valueSet)),
                         combinations(self.vocab.intersection(valueSet), 2))

        for pair in allPairs:
            sortedPair = sorted(pair)
            if sortedPair[0] in value or sortedPair[1] in value:
                i = sortedPair[0] in value
                j = sortedPair[1] in value
                yield (sortedPair[0], sortedPair[1]), ((i and j)+0, i+0, j+0)
                
    def combiner_Jaccard(self, key, values):
        intersect = 0
        i = 0
        j = 0
        for val in values:
            intersect += val[0]
            i += val[1]
            j += val[2]
        yield key, (intersect, i, j)
        
    def reducer_Jaccard(self, key, values):
        intersect = 0.0
        i = 0.0
        j = 0.0
        for val in values:
            intersect += val[0]
            i += val[1]
            j += val[2]
        yield key, intersect / (i + j - intersect)       
    
    """
    Cosine similarity
    - Similar to Jaccard, but do not binarize
    - We can use the same mapper_init as Jaccard
    - For each pair, we have word1 and word2
    - count1 is the matrix entry for word1
    - In the mapper, emit (word1, word2), (count1*count2, count1^2, count2^2)
    - We can use the same combiner as Jaccard
    """
    
    def mapper_Cosine(self, key, value):
        valueSet = set(value.keys())
        allPairs = chain(product(valueSet, self.vocab.difference(valueSet)),
                         combinations(self.vocab.intersection(valueSet), 2))

        for pair in allPairs:
            sortedPair = sorted(pair)
            if sortedPair[0] in value or sortedPair[1] in value:
                if sortedPair[0] in value:
                    i = value[sortedPair[0]]
                else: i = 0.0
                if sortedPair[1] in value:
                    j = value[sortedPair[1]]
                else: j = 0.0
                yield (sortedPair[0], sortedPair[1]), (i*j, i**2, j**2)
                    
    def reducer_Cosine(self, key, values):
        dotprod = 0.0
        i = 0.0
        j = 0.0
        for val in values:
            dotprod += val[0]
            i += val[1]
            j += val[2]
        if i == 0 or j == 0:
            yield key, None
        else:
            yield key, dotprod / ((i ** 0.5) * (j ** 0.5))
    
    """
    Multi-step pipeline definitions
    Based on user input when calling runner function
    """
    def steps(self):
        self.method = self.options.method
        if self.method == 'jaccard':
            return [
                MRStep(mapper=self.mapper_buildFullMatrix,
                       combiner=self.reducer_buildFullMatrix,
                       reducer=self.reducer_buildFullMatrix),
                MRStep(mapper_init=self.mapper_Jaccard_init,
                       mapper=self.mapper_Jaccard,
                       combiner=self.combiner_Jaccard,
                       reducer=self.reducer_Jaccard)
            ]
        elif self.method == 'cosine':
            return [
                MRStep(mapper=self.mapper_buildFullMatrix,
                       combiner=self.reducer_buildFullMatrix,
                       reducer=self.reducer_buildFullMatrix),
                MRStep(mapper_init=self.mapper_Jaccard_init,
                       mapper=self.mapper_Cosine,
                       combiner=self.combiner_Jaccard,
                       reducer=self.reducer_Cosine)
            ]
    

if __name__ == '__main__':
    similarity.run()
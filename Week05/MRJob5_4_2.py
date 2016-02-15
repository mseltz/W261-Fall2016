from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations

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
        word1 = fields[0]
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
    - We need to look at all pairs in the basis
    - We only emit anything if one item in the pair has a value
    - We binarize on whether the co-occurrence support is > 0
    - Emit (word1, word2), (word1 and word2, word1, word2)
    """
    
    def mapper_Jaccard_init(self):
        self.vocab = set()
        with open('basisWords.txt','r') as myfile:
            for word in myfile:
                self.vocab.add(word.strip())
        self.allPairs = list(combinations(sorted(self.vocab),2))
        
    def mapper_Jaccard(self, key, values):
        for pair in self.allPairs:
            i = pair[0] in values
            j = pair[1] in values
            if i or j:
                yield (pair[0], pair[1]), ((i and j)+0, i+0, j+0)
                
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
        i = 0
        j = 0
        for val in values:
            intersect += val[0]
            i += val[1]
            j += val[2]
        yield key, intersect / (i + j - intersect)       
    
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
                       reducer=self.reducer_buildFullMatrix,
                       jobconf={'mapred.reduce.tasks': 2}),
                MRStep(mapper_init=self.mapper_Jaccard_init,
                       mapper=self.mapper_Jaccard,
                       combiner=self.combiner_Jaccard,
                       reducer=self.reducer_Jaccard,
                       jobconf={'mapred.reduce.tasks': 2})
            ]
    

if __name__ == '__main__':
    similarity.run()
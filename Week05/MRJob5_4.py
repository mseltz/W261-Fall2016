from mrjob.job import MRJob
from mrjob.step import MRStep

class stripes(MRJob):
    
    """
    Build stripes
    - Read in basis words from basisWords.txt
    - Emit stripes where the key and each value's key is in the basis
    """
    
    def mapper_buildStripe_init(self):
        self.vocab = set()
        with open('basisWords.txt','r') as myfile:
            for word in myfile:
                self.vocab.add(word.strip())
        
    def mapper_buildStripe(self, _, line):
        fields = line.strip().split('\t')
        words = fields[0].lower().split()
        wordList = sorted(list(set(words)))
        for index1 in range(len(wordList)-1):
            stripe = {}
            if wordList[index1] in self.vocab:
                for index2 in range(index1+1,len(wordList)):
                    if wordList[index2] in self.vocab:
                        stripe[wordList[index2]] = 1
            if len(stripe) > 0:
                yield wordList[index1], stripe
            
    def combiner_buildStripe(self, key, values):
        stripe = {}
        for val in values:
            for word in val:
                if word in stripe:
                    stripe[word] += val[word]
                else:
                    stripe[word] = val[word]
        yield key, stripe
        
    def reducer_buildStripe(self, key, values):
        stripe = {}
        for val in values:
            for word in val:
                if word in stripe:
                    stripe[word] += val[word]
                else:
                    stripe[word] = val[word]
        yield key, stripe
    
            
        
    """
    Multi-step pipeline definitions
    Based on user input when calling runner function
    """
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_buildStripe_init,
                   mapper=self.mapper_buildStripe,
                   combiner=self.combiner_buildStripe,
                   reducer=self.reducer_buildStripe,
                   jobconf={'mapred.reduce.tasks': 2})
        ]
    

if __name__ == '__main__':
    stripes.run()
from mrjob.job import MRJob
from mrjob.step import MRStep

class stripes(MRJob):
    
    """
    Build stripes
    - Read in basis words from basisWords.txt
    - For each 5-gram:
       - Deduplicate the words in the 5-gram, then sort alphabetically
       - Extract count
       - Build stripe: (word1, {word2: x, word3: y, ...})
       
    """
    
    def mapper_buildStripe_init(self):
        self.numDocuments = 0
        self.vocab = set()
        with open('basisWords.txt','r') as myfile:
            for word in myfile:
                self.vocab.add(word.strip())
        
    def mapper_buildStripe(self, _, line):
        fields = line.strip().split('\t')
        words = fields[0].lower().split()
        wordList = sorted(list(set(words)))
        count = float(fields[1])
        self.numDocuments += count
        for index1 in range(len(wordList)-1):
            stripe = {}
            if wordList[index1] in self.vocab:
                for index2 in range(index1+1,len(wordList)):
                    if wordList[index2] in self.vocab:
                        stripe[wordList[index2]] = count
            if len(stripe) > 0:
                yield wordList[index1], stripe
                
    def mapper_buildStripe_final(self):
        yield '*total', {'*total':self.numDocuments}
            
    def combiner_buildStripe(self, key, values):
        stripe = {}
        for val in values:
            for word in val:
                if word in stripe:
                    stripe[word] += val[word]
                else:
                    stripe[word] = val[word]
        yield key, stripe
        
    def reducer_buildStripe_init(self):
        self.numDocs = 0
    
    def reducer_buildStripe(self, key, values):
        stripe = {}
        for val in values:
            for word in val:
                if word in stripe:
                    stripe[word] += val[word]
                else:
                    stripe[word] = val[word]
        if key == '*total':
            self.numDocs = stripe['*total']
        else:
            for word in stripe:
                stripe[word] /= self.numDocs
            yield key, stripe
    
            
        
    """
    Multi-step pipeline definitions
    Based on user input when calling runner function
    """
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_buildStripe_init,
                   mapper=self.mapper_buildStripe,
                   mapper_final=self.mapper_buildStripe_final,
                   combiner=self.combiner_buildStripe,
                   reducer_init=self.reducer_buildStripe_init,
                   reducer=self.reducer_buildStripe,
                   jobconf={'mapred.reduce.tasks': 1})
        ]
    

if __name__ == '__main__':
    stripes.run()
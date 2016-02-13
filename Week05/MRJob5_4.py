from mrjob.job import MRJob
from mrjob.step import MRStep
 
class job(MRJob):
    
    # Specify some custom options so we only have to write one MRJob class for each part
    def configure_options(self):
        super(job, self).configure_options()
        self.add_passthrough_option('--part', default='1')
    
    """
    Find the longest 5-gram
    - In this case, in each mapper, we only need to store the length of the longest 5-gram we have seen
    - After the mapper has run, we emit the longest 5-gram from this mapper
    - All results will be sent to the same reducer (key = None)
    """
    
    def mapper_longest5gram_init(self):
        self.maxLength = 0
    
    def mapper_longest5gram(self, _, line):
        fields = line.strip().split('\t')
        if len(fields[0]) > self.maxLength: 
            self.maxLength = len(fields[0])
            
    def mapper_longest5gram_final(self):
        yield None, self.maxLength
    
    def reducer_longest5gram(self, key, values):
        yield None, max(values)
    
    """
    Top 10 most frequent words
    """

    def mapper_top10words(self, _, line):
        fields = line.strip().split('\t')
        words = fields[0].lower().split()
        print words
    
    # Multi-step pipeline definition
    def steps(self):
        self.part = self.options.part
        if self.part == '1':
            return [
                MRStep(mapper_init=self.mapper_longest5gram_init,
                       mapper=self.mapper_longest5gram,
                       mapper_final=self.mapper_longest5gram_final,
                       reducer=self.reducer_longest5gram)
            ]
        elif self.part == '2':
            return [
                MRStep(mapper=self.mapper_top10words)
            ]

if __name__ == '__main__':
    job.run()
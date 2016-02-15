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
    - All results will be sent to the same reducer (we specify this)
    - Then we loop through the records in the reducer and emit the remaining longest 5-gram
    """
    
    def mapper_longest5Gram_init(self):
        self.maxLength = 0
        self.longest5Gram = None
    
    def mapper_longest5Gram(self, _, line):
        fields = line.strip().split('\t')
        if len(fields[0]) > self.maxLength: 
            self.maxLength = len(fields[0])
            self.longest5Gram = fields[0]
            
    def mapper_longest5Gram_final(self):
        yield self.longest5Gram, self.maxLength
     
    def reducer_longest5Gram_init(self):
        self.maxLength = 0
        self.longest5Gram = None
    
    def reducer_longest5Gram(self, key, values):
        for val in values:
            if val > self.maxLength:
                self.maxLength = val
                self.longest5Gram = key
        
    def reducer_longest5Gram_final(self):
        yield self.maxLength, self.longest5Gram
    
    """
    Top 10 most frequent words
    - This is our standard word count
    - Loop through each word in the 5-gram and emit (word, 1)
    """
    
    def mapper_topWords(self, _, line):
        fields = line.strip().split('\t')
        words = fields[0].lower().split()
        count, pages_count, books_count = int(fields[1]), int(fields[2]), int(fields[3])
        for word in words:
            self.increment_counter('total', 'words', count)
            yield word, count
        
    def combiner_topWords(self, key, values):
        yield key, sum(values)
        
    def reducer_topWords(self, key, values):
        yield key, sum(values)
 
    """
    Densely appearing words
    - For each word, emit count and pages_count
    - Combiner sums count and pages_count
    - Reducer sums count and pages_count, then emits count/pages_count
    """
    
    def mapper_denseWords(self, _, line):
        fields = line.strip().split('\t')
        words = fields[0].lower().split()
        count, pages_count, books_count = int(fields[1]), int(fields[2]), int(fields[3])
        for word in words:
            yield word, (count, pages_count)
        
    def combiner_denseWords(self, key, values):
        count, pages_count = 0.0, 0.0
        for val in values:
            count += val[0]
            pages_count += val[1]
        yield key, (count, pages_count)
        
    def reducer_denseWords(self, key, values):
        count, pages_count = 0.0, 0.0
        for val in values:
            count += val[0]
            pages_count += val[1]
        yield key, count/pages_count

    """
    Frequent5-grams
    - Use count to determine the most frequent 5-gram
    - Sum counts in combiner and reducer
    """
        
    def mapper_frequent5Gram(self, _, line):
        fields = line.strip().split('\t')
        yield len(fields[0]), float(fields[1])
        
    def combiner_frequent5Gram(self, key, values):
        yield key, sum(values)
        
    def reducer_frequent5Gram(self, key, values):
        yield key, sum(values)
        
    """
    Sorting functions
    - We need these to get the top and bottom values
    - Utilize only one reducer instead of writing a custom partitioner
    """
    def mapper_sort(self, key, value):
        yield float(value), key
        
    def reducer_sort_init(self):
        self.count = 0
    
    def reducer_top10(self, key, values):
        for val in values:
            if self.count < 10:
                yield key, val
                self.count += 1
                
    def reducer_top100(self, key, values):
        for val in values:
            if self.count < 100:
                yield key, val
                self.count += 1
                
    def reducer_top10000(self, key, values):
        for val in values:
            if self.count < 10000:
                yield key, val
                self.count += 1
                
    def reducer_all(self, key, values):
        for val in values:
            yield key, val

    """
    Multi-step pipeline definitions
    Based on user input when calling runner function
    """
    def steps(self):
        self.part = self.options.part
        if self.part == '1':
            return [
                MRStep(mapper_init=self.mapper_longest5Gram_init,
                       mapper=self.mapper_longest5Gram,
                       mapper_final=self.mapper_longest5Gram_final,
                       reducer_init=self.reducer_longest5Gram_init,
                       reducer=self.reducer_longest5Gram,
                       reducer_final=self.reducer_longest5Gram_final,
                       jobconf={'mapred.reduce.tasks': 1})
            ]
        elif self.part == '2':
            return [
                MRStep(mapper=self.mapper_topWords,
                       combiner=self.combiner_topWords,
                       reducer=self.reducer_topWords),
                MRStep(mapper=self.mapper_sort,
                       reducer_init=self.reducer_sort_init,
                       reducer=self.reducer_all,
                       jobconf={'mapred.output.key.comparator.class':'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                                'mapred.text.key.partitioner.options':'-k1,1',
                                'stream.num.map.output.key.fields':1,
                                'mapred.text.key.comparator.options':'-k1,1nr',
                                'mapred.reduce.tasks': 1})
            ]
        elif self.part == '3':
            return [
                MRStep(mapper=self.mapper_denseWords,
                       combiner=self.combiner_denseWords,
                       reducer=self.reducer_denseWords),
                MRStep(mapper=self.mapper_sort,
                       reducer_init=self.reducer_sort_init,
                       reducer=self.reducer_all,
                       jobconf={'mapred.output.key.comparator.class':'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                                'mapred.text.key.partitioner.options':'-k1,1',
                                'stream.num.map.output.key.fields':1,
                                'mapred.text.key.comparator.options':'-k1,1nr',
                                'mapred.reduce.tasks': 1})
            ]
        elif self.part == '4':
            return [
                MRStep(mapper=self.mapper_frequent5Gram,
                       combiner=self.combiner_frequent5Gram,
                       reducer=self.reducer_frequent5Gram),
                MRStep(mapper=self.mapper_sort,
                       reducer_init=self.reducer_sort_init,
                       reducer=self.reducer_all,
                       jobconf={'mapred.output.key.comparator.class':'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                                'mapred.text.key.partitioner.options':'-k1,1',
                                'stream.num.map.output.key.fields':1,
                                'mapred.text.key.comparator.options':'-k1,1nr',
                                'mapred.reduce.tasks': 1})
            ]


if __name__ == '__main__':
    job.run()
from __future__ import division
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONProtocol
from math import log, exp

class topN(MRJob):
    
    #------------------
    # Configurations:
    
    def configure_options(self):
        super(topN, self).configure_options()
        self.add_passthrough_option('--top', default=10, type='int')

    
    INPUT_PROTOCOL = JSONProtocol
    
    #------------------
    # Mapper:
    # - Throw out graph structure
    # - Use PageRank as key
    
    def mapper(self, key, value):
        yield log(value[1]), key
        
    #------------------
    # Reducer:
    # - Take top N values
    
    def reducer_init(self):
        self.seen = 0
    
    def reducer(self, key, values):
        n = self.options.top
        
        for val in values:
            if self.seen < n:
                yield exp(key), val
                self.seen += 1
                
    #------------------
    # Pipeline:
    
    def steps(self):
        return [MRStep(mapper=self.mapper,
                       reducer_init=self.reducer_init,
                       reducer=self.reducer,
                       jobconf={'mapred.output.key.comparator.class':'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                                'mapred.text.key.partitioner.options':'-k1,1',
                                'stream.num.map.output.key.fields':1,
                                'mapred.text.key.comparator.options':'-k1,1nr',
                                'mapred.reduce.tasks': 1
                               })]

        
if __name__ == '__main__':
    topN.run()
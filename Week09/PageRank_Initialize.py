from __future__ import division
from mrjob.job import MRJob
from mrjob.step import MRStep

class initialize(MRJob):
    
    """
    Get all nodes
    """
    
    #------------------
    # Mapper:
    # - We need to make sure we emit a line for each node in the graph
    # - Right now there are no lines for nodes with no neighbors
    
    def mapper(self, _, line):
        
        # Split fields
        
        fields = line.split('\t')
        key = fields[0]
        stripe = eval(fields[1])
        
        # Emit the key and stripe
        
        yield key, stripe
        
        # For each neighbor, emit a 0
        # We just do this so we catch all nodes
        
        for neighbor in stripe:
            yield neighbor, 0
            
    #------------------
    # Reducer:
    # - We need to deduplicate each of our nodes
    # - If we encounter a value that is a dictionary, these are the neighbors
    # - If we do not encounter any dictionaries, then the node is dangling, we emit an empty neighbor list
    
    def reducer(self, key, values):       
        stripe = {}
        
        # Loop through values for a key to see if it has neighbors
        # If it does, we need to keep the neighbors
        
        for val in values:
            if type(val) == type(stripe):
                stripe = val
                
        # For each key, emit only one thing, which is the neighbor list
        # We should now have a line for each node, even if the neighbor list is empty
        
        yield key, stripe
        
    """
    Normalize length
    """
    
    # Initialize total to 0
    def mapper_norm_init(self):
        self.total = 0.0
    
    # For each key we encounter, increment total
    # We know that we will only encounter each node once
    def mapper_norm(self, key, value):
        yield key, value
        self.total += 1
        
    # Emit the total number of nodes we saw
    def mapper_norm_final(self):
        yield '*', self.total
    
    # To combine the totals if we have multiple mappers
    def combiner_norm(self, key, values):
        if key == '*':
            yield key, sum(values)
        else:
            for val in values:
                yield key, val
        
    # Initialize the totalNodes to 0
    def reducer_norm_init(self):
        self.totalNodes = 0
       
    # If the key is '*', save the sum of the values
    # Otherwise, yield the key, stripe, and 1/n
    def reducer_norm(self, key, values):
        if key == '*':
            self.totalNodes = sum(values)
        else:
            for val in values:
                yield key, (val, 1 / self.totalNodes)
    
    """
    Multi-step pipeline
    """
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper_init=self.mapper_norm_init,
                   mapper=self.mapper_norm,
                   mapper_final=self.mapper_norm_final,
                   combiner=self.combiner_norm,
                   reducer_init=self.reducer_norm_init,
                   reducer=self.reducer_norm,
                   jobconf={'mapred.reduce.tasks': 1})
        ]

if __name__ == '__main__':
    initialize.run()
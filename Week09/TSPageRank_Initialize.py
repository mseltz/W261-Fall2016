from __future__ import division
from mrjob.job import MRJob
from mrjob.step import MRStep

class initialize(MRJob):
        
    #------------------
    # Configurations: 
    
    def configure_options(self):
        super(initialize, self).configure_options()
        self.add_passthrough_option('--beta', default=0.99, type='float')
        self.add_passthrough_option('--topic', default='1', type='string')
    
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
    Initialize topic weights (v_ji)
    Normalize length
    """
    
    #------------------
    # Mapper:
    # - Find total number of nodes
    
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
    
    #------------------
    # Combiner:
    # - Partial sum of total nodes
    
    # To combine the totals if we have multiple mappers
    def combiner_norm(self, key, values):
        if key == '*':
            yield key, sum(values)
        else:
            for val in values:
                yield key, val
     
    #------------------
    # Reducer:
    # - Partial sum of total nodes
    # - Calculate weight vector (v_ji) for each node
    
    # Initialize the totalNodes to 0
    # Read topics and topicCount (only for topic of interest) into memory
    def reducer_norm_init(self):
        self.totalNodes = 0
        self.topics = {}
        self.topicCount = 0.0
        
        with open('randNet_topics.txt', 'r') as f1:
            for line in f1:
                fields = line.strip().split('\t')
                node = fields[0]
                topic = fields[1]
                self.topics[node] = topic
        
        with open('randNet_topicCount.txt', 'r') as f2:
            for line in f2:
                fields = line.strip().split('\t')
                topic = fields[0]
                count = eval(fields[1])
                if topic == self.options.topic:
                    self.topicCount = count
       
    # If the key is '*', save the sum of the values
    # Otherwise, yield the key, stripe, PageRank (1/n) and weight
    def reducer_norm(self, key, values):
        if key == '*':
            self.totalNodes = sum(values)
        else:
            
            # Is this key is in the topic of interest?
            keyInTopic = self.topics[key] == self.options.topic
            
            # If the key is part of our topic, weight = beta / size of topic
            if keyInTopic:
                weight = self.options.beta / self.topicCount
            
            # Otherwise, weight = (1 - beta) / size of not-topic
            else:
                weight = (1 - self.options.beta) / (self.totalNodes - self.topicCount)
                
            for val in values:
                yield key, (val, 1 / self.totalNodes, weight)
    
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
                   jobconf={'mapreduce.job.reduces': 1})
        ]

if __name__ == '__main__':
    initialize.run()
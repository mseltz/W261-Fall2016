from __future__ import division
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONProtocol

class iterate(MRJob):
    
    
    """
    Configurations
    """
    
    def configure_options(self):
        super(iterate, self).configure_options()
        self.add_passthrough_option('--numNodes', default=1, type='int')
        self.add_passthrough_option('--alpha', default=0.15, type='float')
    
    """
    Mapper: Distribute PageRank mass to all neighbors
    - Do not account for teleportation yet
    """
    
    INPUT_PROTOCOL = JSONProtocol
    
    #------------------
    # Mapper:
    # - Find the number of neighbors for the node
    # - Distribute current PageRank among all neighbors
    # - If there are no neighbors, keep track of dangling mass
    
    def mapper_dist(self, key, value):

        # Divide the current PageRank by the number of neighbors
        
        numNeighbors = len(value[0])
        PageRank = value[1]
        
        # If there are neighbors, distribute the PageRank to each neighbors
        
        if numNeighbors > 0:
            for neighbor in value[0]:
                yield neighbor, PageRank / numNeighbors
                
        # If there are no neighbors, we need to account for this dangling node
        
        else:
            yield '*dangling', PageRank
        
        # Maintain the graph structure
        
        yield key, value[0]
     
    #------------------
    # Reducer:
    # - For each node, accumulate PageRank distributed from other nodes
    # - Maintain graph structure
    
    def reducer_dist(self, key, values):
        
        new_PageRank = 0.0
        neighbors = {}
        
        for val in values:
            if type(val) == type(0.0):
                new_PageRank += val
            elif type(val) == type({}):
                neighbors = val
        
        if key == '*dangling':
            with open('danglingMass.txt', 'w') as myfile:
                myfile.write(str(new_PageRank))
        else:
            yield key, (neighbors, new_PageRank)

    #------------------
    # Mapper: 
    # - Account for teleportation
    # - Distribute dangling mass to all nodes
    
    def mapper_dangle_init(self):
        with open('danglingMass.txt', 'r') as f:
            for line in f:
                self.m = float(line)
    
    def mapper_dangle(self, key, value):
        a = self.options.alpha
        n = self.options.numNodes
        new_PageRank = a * (1 / n) + (1 - a) * (self.m / n + value[1])
        yield key, (value[0], new_PageRank)
    
            
    """
    Multi-step pipeline
    """
    def steps(self):
        return [
            MRStep(mapper=self.mapper_dist,
                   reducer=self.reducer_dist),
            MRStep(mapper_init=self.mapper_dangle_init,
                   mapper=self.mapper_dangle)
            ]

if __name__ == '__main__':
    iterate.run()
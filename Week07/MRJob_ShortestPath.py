from mrjob.job import MRJob
from mrjob.step import MRStep
import sys

class shortestPath(MRJob):
    
    """
    Mapper: Iterate over each node in graph file
    - Expand frontier if needed
    - Update node statuses
    """
    def mapper(self, _, line):
        
        # Split text to get our data
        fields = line.strip().split('\t')
        name = fields[0]
        value = eval(fields[1])
        neighbors = value[0]
        distance = int(value[1])
        status = value[2]
        path = value[3]
        
        # If this node is queued, expand the frontier
        #  - mark current node as visited
        #  - yield neighbor nodes into queue
        if status == 'Q':
            yield name, [neighbors, distance, 'V', path]
            if neighbors:
                for node in neighbors:
                    temp_path = list(path)
                    temp_path.append(node)
                    yield node, [None, distance + 1, 'Q', temp_path]
        else:
            yield name, [neighbors, distance, status, path]
        
        
    """
    Reducer: Aggregate expanded nodes
    """
    def reducer(self, key, values):
        neighbors = None
        distance = sys.maxint
        status = None
        path = []
        
        for val in values:
            
            # We've hit a visited node. Break out of the loop.
            if val[2] == 'V':
                neighbors = val[0]
                distance = val[1]
                status = val[2]
                path = val[3]
                break
            
            # We've hit an unvisited node. Collect the neighbors and the status
            # If status is already Q, do not overwrite
            elif val[0]: 
                neighbors = val[0]
                if status != 'Q':
                    status = val[2]
            
            # We've hit a queued node. Update status and path
            else:
                status = val[2]
                path = val[3]
                
            # Update minimum distance if necessary
            distance = min(distance, val[1])
            
        yield key, [neighbors, distance, status, path]
    
    """
    Multistep pipeline definition
    """
#     def steps(self):
#         return [
#                 MRStep()
#             ]
    
if __name__ == '__main__':
    shortestPath.run()
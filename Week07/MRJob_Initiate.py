from mrjob.job import MRJob
from mrjob.step import MRStep
import sys

class initiate(MRJob):
        
    # Specify some custom options so we only have to write one MRJob class for each join
    def configure_options(self):
        super(initiate, self).configure_options()
        self.add_passthrough_option('--startNode', default='1')
        
    def mapper(self, _, line):
        fields = line.strip().split('\t')
        name = fields[0]
        neighbors = eval(fields[1])
        if name == self.options.startNode:
            yield name, [neighbors, 0, 'Q', [name]]
        else:
            yield name, [neighbors, sys.maxint, 'U', []]
        
if __name__ == '__main__':
    shortestPath.run()
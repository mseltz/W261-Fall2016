from mrjob.job import MRJob
from mrjob.step import MRStep

class shortestPath(MRJob):
    
    # Specify some custom options so we only have to write one MRJob class for each join
    def configure_options(self):
        super(join, self).configure_options()
        self.add_passthrough_option('--startNode', default='1')
    
    def mapper(self, _, line):
        yield None, line
        
        
    """
    Multistep pipeline definition
    """
#     def steps(self):
#         return [
#                 MRStep()
#             ]
    
if __name__ == '__main__':
    shortestPath.run()
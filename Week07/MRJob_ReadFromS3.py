from mrjob.job import MRJob
from mrjob.step import MRStep

class job(MRJob):
        
    # Specify some custom options so we only have to write one MRJob class for each join
    def configure_options(self):
        super(job, self).configure_options()
        self.add_passthrough_option('--endNode', default='1')
        self.add_passthrough_option('--startNode', default='1')
        
    def mapper(self, _, line):

        # Split text to get our data
        fields = line.strip().split('\t')
        
        # If using EMR, need to eval the string
        name = eval(fields[0])
        value = eval(fields[1])
        neighbors = value[0]
        distance = int(value[1])
        status = value[2]
        path = value[3]
        
        if name == self.options.endNode or name == self.options.startNode:
            yield name, [neighbors, distance, status, path]
        
    def reducer(self, key, values):
        for val in values:
            yield key, val
        
if __name__ == '__main__':
    job.run()
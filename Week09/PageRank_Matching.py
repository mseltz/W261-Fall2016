from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONProtocol

class matching(MRJob):
    
    #------------------
    # Configurations:
    
    def configure_options(self):
        super(matching, self).configure_options()
        self.add_passthrough_option('--f', default='wikipediaTop100_5.txt', type='string')
    
    INPUT_PROTOCOL = JSONProtocol
    
    #------------------
    # Mapper:
    # - Stream through one file and only keep entries
    #   matching indexes in the included file
    
    def mapper_init(self):
        self.pages = []
        with open(self.options.f + '.txt', 'r') as f:
            for line in f:
                fields = line.strip().split('\t')
                page = eval(fields[1])
                self.pages.append(page)
            
    def mapper(self, key, value):
        if key in self.pages:
            yield key, value[1]
        
    #------------------
    # Pipeline:
    
    def steps(self):
        return [MRStep(mapper_init=self.mapper_init,
                       mapper=self.mapper)]

        
if __name__ == '__main__':
    matching.run()
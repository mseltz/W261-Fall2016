from mrjob.job import MRJob
from mrjob.step import MRStep
 
class join(MRJob):
    
    # Specify some custom options so we only have to write one MRJob class for each join
    def configure_options(self):
        super(join, self).configure_options()
        self.add_passthrough_option('--joinType', default='inner', )
    
    # Store attributes.csv into memory
    #  - account for multiple occurrences of keys
    #  - self.pages is dict with a list of [pageName, pageURL] pairs
    # Set joinType variable
    def mapper_init(self):
        self.pages = {}
        with open('attributes.csv','r') as myfile:
            for line in myfile:
                fields = line.strip().split(',')
                if fields[0] not in self.pages:
                    self.pages[fields[0]]=[]
                self.pages[fields[0]].append([fields[1], fields[2]])
        self.joinType = self.options.joinType
        self.seenRight = set()
    
    # RIGHT table is stored in memory (self.pages)
    # LEFT table is streamed
    # We need so keep track of which RIGHT keys we have seen
    def mapper(self, _, line):
        fields = line.split(',')
        key = fields[0]
        self.seenRight.add(key)
        if key in self.pages:
            for i in self.pages[key]:
                yield key, (fields[1], fields[2], i[0], i[1])
        elif self.joinType == 'left':
            yield key, (fields[1], fields[2], None, None)

    # We need to emit all of the RIGHT keys that we never saw while streaming through LEFT
    # We will need to deduplicate these in the reducer in case we have multiple mappers
    def mapper_final(self):
        if self.joinType == 'right':
            for key in self.pages:
                if key not in self.seenRight:
                    for value in self.pages[key]:
                        yield key, (None, None, value[0], value[1])
    
    # Need to persist variables
    def reducer_init(self):
        self.joinType = self.options.joinType
    
    # We need to unpack and emit each record
    # We also need to do some work emitting records for the right join
    def reducer(self, key, values):
        emptyRight = True
        for val in values:
            if self.joinType == 'inner' or self.joinType == 'left':
                yield key, val
            elif self.joinType == 'right':
                if val[:2] != [None]*2:
                    emptyRight = False
                    yield key, val
                else: emptyRecord = val
        if emptyRight and self.joinType == 'right':
            yield key, emptyRecord


if __name__ == '__main__':
    join.run()
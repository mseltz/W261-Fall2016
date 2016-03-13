from mrjob.job import MRJob
from mrjob.step import MRStep

class explore(MRJob):
        
    # Specify some custom options so we only have to write one MRJob class for each join
    def configure_options(self):
        super(explore, self).configure_options()
        self.add_passthrough_option('--exploreType', default='nodes')
        
    """
    Find number of nodes
    """
    def mapper_discoverNodes(self, _, line):
        fields = line.strip().split('\t')
        name = fields[0]
        neighbors = eval(fields[1])
        yield name, 1
        if neighbors:
            for node in neighbors:
                yield node, 1
        
    def reducer_discoverNodes(self, key, values):
        yield key, 1
        
    def mapper_countNodes(self, key, value):
        yield None, 1
        
    def reducer_countNodes(self, key, values):
        yield None, sum(values)
    
    """
    Find number of links
    """
    def mapper_links(self, _, line):
        fields = line.strip().split('\t')
        name = fields[0]
        neighbors = eval(fields[1])
        if neighbors:
            for node in neighbors:
                yield None, 1
        
    def reducer_links(self, key, values):
        yield None, sum(values)
    
    """
    Find average in-degree and out-degree
    """
    
    # For the mapper, we need to emit number of out and in links
    # Count number of neighbors to find num links going out
    # Count 1 for each node in neighbor list to find num links coming in
    # Yield in the form of nodeName, (inLinks, outLinks)
    def mapper_degrees(self, _, line):
        fields = line.strip().split('\t')
        name = fields[0]
        neighbors = eval(fields[1])
        
        # Yield number of links going out
        yield name, (0, len(neighbors))
        
        # Yield 1 link coming in for each node in neighbors
        if neighbors:
            for node in neighbors:
                yield node, (1, 0)
            
    def reducer_degrees(self, key, values):
        inSum = 0
        outSum = 0
        for val in values:
            inSum += val[0]
            outSum += val[1]
        yield key, (inSum, outSum)
        
    def mapper_degreesAvg(self, key, value):
        yield None, (value[0], value[1], 1)
        
    def combiner_degreesAvg(self, key, values):
        count = 0
        inSum = 0
        outSum = 0
        for val in values:
            inSum += val[0]
            outSum += val[1]
            count += 1
        yield None, (inSum, outSum, count)
        
    def reducer_degreesAvg(self, key, values):
        count = 0.0
        inSum = 0.0
        outSum = 0.0
        for val in values:
            inSum += val[0]
            outSum += val[1]
            count += val[2]
        yield None, (inSum / count, outSum / count)
        
    """
    Multi-step pipeline
    """
    def steps(self):
        if self.options.exploreType == 'nodes':
            return [
                MRStep(mapper=self.mapper_discoverNodes,
                       combiner=self.reducer_discoverNodes,
                       reducer=self.reducer_discoverNodes),
                MRStep(mapper=self.mapper_countNodes,
                       combiner=self.reducer_countNodes,
                       reducer=self.reducer_countNodes)
            ]
        elif self.options.exploreType == 'links':
            return [
                MRStep(mapper=self.mapper_links,
                       combiner=self.reducer_links,
                       reducer=self.reducer_links)
            ]
        elif self.options.exploreType == 'degrees':
            return [
                MRStep(mapper=self.mapper_degrees,
                       combiner=self.reducer_degrees,
                       reducer=self.reducer_degrees),
                MRStep(mapper=self.mapper_degreesAvg,
                       combiner=self.combiner_degreesAvg,
                       reducer=self.reducer_degreesAvg)
            ]

        
if __name__ == '__main__':
    explore.run()
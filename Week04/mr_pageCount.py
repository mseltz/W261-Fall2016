from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

def read_csvLine(line):
    # Given a comma delimited string, return fields
    for row in csv.reader([line]):
        return row

class MRPageFreqCount(MRJob):
    
    # Mapper1: emit page_id, 1
    def mapper_count(self, _, line):
        fields = read_csvLine(line)
        yield fields[1], 1

    # Combiner1: aggregate page_id
    def combiner_count(self, page, counts):
        yield page, sum(counts)

    # Reducer1: aggregate page_id
    def reducer_count(self, page, counts):
        yield page, sum(counts)
    
    # Mapper2: invert page and counts to sort
    def mapper_sort(self, page, counts):
        yield '%010d' % counts, page
        
    # Reducer2: identity
    def reducer_sort(self, counts, page):
        for p in page:
            yield int(counts), p
                
    # Multi-step pipeline definition
    def steps(self):
        return [
            MRStep(mapper=self.mapper_count, 
                   combiner=self.combiner_count, 
                   reducer=self.reducer_count),
            MRStep(mapper=self.mapper_sort,
                   reducer=self.reducer_sort)]
    
    
    

if __name__ == '__main__':
    MRPageFreqCount.run()
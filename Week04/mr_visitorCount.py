from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import Counter
import csv

def read_csvLine(line):
    # Given a comma delimited string, return fields
    for row in csv.reader([line]):
        return row

class MRTopVisitorCount(MRJob):
    
    # Mapper1: emit page_id, 1
    def mapper_count(self, _, line):
        fields = read_csvLine(line)
        yield fields[1], (fields[4], 1)

    # Reducer1: aggregate page_id
    def reducer_count(self, page, counts):
        count = Counter()
        for visitor in counts:
            count.update({visitor[0]:visitor[1]})
        yield page, count
    
    # Mapper2: invert page and counts to sort
    def mapper_sort(self, page, counts):
        top = Counter(counts).most_common(1)
        yield page, (top[0][0], top[0][1])
        
    # Reducer2: identity
    def reducer_sort(self, page, visitor_count):
        for v in visitor_count:
            yield page, v
                
    # Multi-step pipeline definition
    def steps(self):
        return [
            MRStep(mapper=self.mapper_count,
                   reducer=self.reducer_count),
            MRStep(mapper=self.mapper_sort,
                   reducer=self.reducer_sort)]
    
    
    

if __name__ == '__main__':
    MRPageFreqCount.run()
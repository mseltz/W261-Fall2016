from mrjob.job import MRJob
from mrjob.step import MRStep

class WOLSviaGD_Batch(MRJob):
    
    """
    Initialize coefficients
    - coefficients are in coefs.txt
    """
    def initializeCoefs(self):
        
        # Read in coefficients
        with open('coefs.txt','r') as f:
            self.coefs = [float(x) for x in f.readline().split(',')]
        
        # Initialize gradient for this iteration
        self.partialGradient = [0]*len(self.coefs)
        self.partialCount = 0
    
    """
    Calculate partial gradient for each example
    - we need to multiply by the weight in WOLS
    - keep partial sums in memory
    """
    def calculatePartialGradient(self, _, line):
        
        # D is one observation of our data
        # The observations are in the form (y, x, weight)
        D = (map(float, line.split(',')))
        y = D[0]
        x = D[1]
        weight = D[2]
        
        # yHat is the predicted value given current coefficients
        yHat = self.coefs[0] + self.coefs[1] * x
        
        # Update partial gradient with gradient from D
        self.partialGradient = [self.partialGradient[0] + (y - yHat) * weight,
                                self.partialGradient[1] + (y - yHat) * x * weight]
        self.partialCount += 1
        
    """
    Yield partial gradient when mapper is done
    """
    def emitPartialGradient(self):
        yield None, (self.partialGradient, self.partialCount)
        
    """
    Aggregate all partial gradients to output gradient vector
    """
    def calculateGradient(self, _, partialGradientRecords):
        
        # Initialize totals
        totalGradient = [0]*2
        totalCount = 0

        # Accumulate
        for partialGradient, partialCount in partialGradientRecords:
            totalCount += partialCount
            for i in range(len(totalGradient)):
                totalGradient[i] += partialGradient[i]
        
        # Emit total gradient
        yield None, [x / totalCount for x in totalGradient]
        
    """
    Multistep pipeline definition
    """
    def steps(self):
        return [
                MRStep(mapper_init=self.initializeCoefs,
                       mapper=self.calculatePartialGradient,
                       mapper_final=self.emitPartialGradient,
                       reducer=self.calculateGradient)
            ]
    
if __name__ == '__main__':
    WOLSviaGD_Batch.run()
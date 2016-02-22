from mrjob.job import MRJob
import numpy as np
import json
from math import pi, sqrt, exp, pow, log
from decimal import *

# Helper function to determine probability of x
# given mean (mu)
# See equation (0)
def bernoulliLogProb(x, mu):
    # Use logs here!
    n = len(x)
    logProb = 0
    for i in range(n):
        if mu[i] ** x[i] * (1 - mu[i]) ** (1 - x[i]) == 0:
            logProb = float('-inf')
            break
        elif mu[i] ** x[i] * (1 - mu[i]) ** (1 - x[i]) == 1:
            logProb += 0.0
        else:
            logProb += x[i] * log(mu[i]) + (1 - x[i]) * log(1 - mu[i])
    return logProb

class BMM_EM_Iterate(MRJob):
    
    """
    Job configuration details
    """
    # Set default protocol
    DEFAULT_PROTOCOL='json'
    
    # Define initializer
    def __init__(self, *args, **kwargs):
        super(BMM_EM_Iterate, self).__init__(*args, **kwargs)
        
        # Read input from JSON
        fullPath = self.options.pathName + 'intermediateResults.txt'
        with open(fullPath,'r') as infile:
            inputJSON = infile.read()
        inputList = json.loads(inputJSON)
        
        # Initialize prior class probabilities (phi), centroids and covariances
        self.phi = np.array(inputList[0])
        self.centroids = np.array(inputList[1])
        
        # Initialize partial sums
        self.new_phi = np.zeros_like(self.phi)
        self.new_centroids = np.zeros_like(self.centroids)
        
        # Set number of mappers
        self.numMappers = 1
        
        # Initialize count
        self.count = 0
        
    # Configure job options
    def configure_options(self):
        super(BMM_EM_Iterate, self).configure_options()
        self.add_passthrough_option('--k', dest='k', default=3, type='int', 
                                    help='k: number of densities in mixture')
        self.add_passthrough_option('--pathName', dest='pathName', default='', type='str',
                                    help='pathName: path name where intermediateResults.txt is stored')
        
    """
    Mapper
    - accumulate partial sums
    """
    def mapper(self, _, line):
        
        k = self.options.k
        
        #############################################
        # Expectation step: equation (1)
        # Calculated for each data point
        # weightVector[k] = p(w_k | x(i), theta)
        # where i is data point and k is the component
        #############################################
        
        x = np.array(json.loads(line))
        weightVector = np.zeros_like(self.phi)
        for i in range(k):
            weightVector[i] = Decimal(log(self.phi[i]) + bernoulliLogProb(x, self.centroids[i])).exp()
        
        if sum(weightVector) == 0:
            weightVector += .0001
        weightVector /= sum(weightVector)
        
        #############################################
        # Maximization step part 1: 
        # Partial sums for equations (2), (3) and (4)
        #############################################
        
        self.count += 1
        
        # Equation (4)
        self.new_phi += weightVector
        
        # Equation (2): partial sum for weighted xs
        for i in range(k):
            self.new_centroids[i] += weightVector[i] * x

        
    """
    Mapper Final
    - emit partially accumulated count, phi, centroids and covariances
    """
    def mapper_final(self):
        out = [self.count, (self.new_phi).tolist(), (self.new_centroids).tolist()]
        jOut = json.dumps(out)
        yield 1, jOut
    
    """
    Reducer
    - accumulate partial sums of count, phi, centroids and covariances
    - each of phi, centroids and covariance inverses are length k
    - divide
    """
    def reducer(self, key, values):
        k = self.options.k
        first = True
        
        for val in values:
            
            # If this is the first record, initialize sums
            if first:
                fields = json.loads(val)
                
                totalCount = fields[0]
                totalPhi = np.array(fields[1])
                totalCentroids = np.array(fields[2])
                first = False
            
            # If this isn't the first, accumulate
            else:
                fields = json.loads(val)
                
                totalCount += fields[0]
                totalPhi += np.array(fields[1])
                totalCentroids += np.array(fields[2])
        
        # Finally divide
        # This finalizes equation (4)
        finalPhi = totalPhi / totalCount
        
        # Make a copy so that we know these are the right dimensions
        finalCentroids = np.array(totalCentroids)
   
        # This finalizes equation (2)
        for i in range(k):
            finalCentroids[i, :] = totalCentroids[i, :] / totalPhi[i]
            
        # Create output
        outputList = [finalPhi.tolist(), finalCentroids.tolist()]
        jsonOut = json.dumps(outputList)
                
        # Write to file
        fullPath = self.options.pathName + 'intermediateResults.txt'
        with open(fullPath, 'w') as outfile:
            outfile.write(jsonOut)
            
    
if __name__ == '__main__':
    GMM_EM_Iterate.run()
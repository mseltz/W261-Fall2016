from mrjob.job import MRJob
import numpy as np
from random import sample
import json
from math import pi, sqrt, exp, pow

class GMM_EM_Initialize(MRJob):
    
    """
    Job configuration details
    """
    # Set default protocol
    DEFAULT_PROTOCOL='json'
    
    # Define initializer
    def __init__(self, *args, **kwargs):
        super(GMM_EM_Initialize, self).__init__(*args, **kwargs)
        
        # Set number of mappers
        self.numMappers = 1
        
        # Initialize count
        self.count = 0
        
    # Configure job options
    def configure_options(self):
        super(GMM_EM_Initialize, self).configure_options()
        self.add_passthrough_option('--k', dest='k', default=3, type='int', 
                                    help='k: number of densities in mixture')
        self.add_passthrough_option('--pathName', dest='pathName', default='', type='str',
                                    help='pathName: path name where intermediateResults.txt is stored')
        
    """
    Mapper
    - Output a few data points to initialize centroids
    """
    def mapper(self, _, line):
        
        # We are going to just output 2 * k points
        if self.count < 2 * self.options.k:
            self.count += 1
            yield (1, line)
            
    """
    Reducer
    - Essentially, this is an identity reducer, but we need to initialize centroids
    - Collect the 2 * k points from the mapper, choose k of them for starting points
    - Emit all values
    """
    def reducer(self, key, values):
        
        #############################################
        # Initialize centroids
        #############################################
        
        k = self.options.k
        sampleCentroids = []
        
        # Append each point from the mapper to centroids
        # After appending, emit them
        for val in values:
            sampleCentroids.append(json.loads(val))
            yield 1, val
            
        # Sample k points from the sample centroids list
        sampleIndex = sample(range(len(sampleCentroids)), k)
        centroids = []
        for i in sampleIndex:
            centroids.append(sampleCentroids[i])
        
        # centroids = our starting centroid points
        
        
        #############################################
        # Use the covariance of the centroids as the 
        # starting guess for covariance
        #############################################
        
        # 1. Calculate mean of centroids
        means = np.array(centroids[0])
        
        for i in range(1, k):
            means += np.array(centroids[i])
        
        means /= float(k)
        
        # 2. Accumulate the deviations
        covariance = np.zeros((len(means), len(means)), dtype=float)
        
        for c in centroids:
            deviation = np.array(c) - means
            
            for i in range(len(means)):
                covariance[i, i] += deviation[i] ** 2
        
        covariance /= float(k)
        
        # Get the inverse of the covariance matrix
        covInvOne = np.linalg.inv(covariance)
        
        # Repeat covariance inverse k times
        # We will have a covariance inverse for each centroid
        covInvs = [covInvOne.tolist()]*k
        
        # For debugging purposes
        jsonDebug = json.dumps([centroids, 
                                means.tolist(), 
                                covariance.tolist(), 
                                covInvOne.tolist(),
                                covInvs])
        debugPath = self.options.pathName + 'debug.txt'
        with open(debugPath, 'w') as outfile:
            outfile.write(jsonDebug)
        
        
        #############################################
        # Use 1/k as the starting guess for the phis
        #############################################
        
        phi = np.zeros(k, dtype=float)
        
        for i in range(k):
            phi[i] = 1.0/float(k)
            
            
        #############################################
        # Output our initializations
        #############################################
        
        outputList = [phi.tolist(), centroids, covInvs]
        jsonOut = json.dumps(outputList)
        
        # Write to file
        fullPath = self.options.pathName + 'intermediateResults.txt'
        with open(fullPath, 'w') as outfile:
            outfile.write(jsonOut)
            
        
if __name__ == '__main__':
    GMM_EM_Initialize.run()
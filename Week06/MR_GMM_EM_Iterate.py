from mrjob.job import MRJob
import numpy as np
import json
from math import pi, sqrt, exp, pow

# Helper function to determine probability of x
# given mean (mu) and inverse covariance (covInv)
# See equation (0)
def gaussProb(x, mu, covInv):
    
    # Get length of array
    n = len(x)
    
    # Subtract the mean from x
    xMinusMu = x - mu
    
    # Calculate numerator
    numerator = exp(-0.5*np.dot(xMinusMu, np.dot(covInv, xMinusMu)))
    
    # Calculate denominator
    # - need the determinant of covariance (detCov)
    # - det(A) = 1/det(A_inverse)
    detCov = 1/np.linalg.det(covInv)
    denominator = (pow(2.0 * pi, n / 2.0) * sqrt(detCov))
    
    # Calculate probability
    return numerator / denominator

class GMM_EM_Iterate(MRJob):
    
    """
    Job configuration details
    """
    # Set default protocol
    DEFAULT_PROTOCOL='json'
    
    # Define initializer
    def __init__(self, *args, **kwargs):
        super(GMM_EM_Iterate, self).__init__(*args, **kwargs)
        
        # Read input from JSON
        fullPath = self.options.pathName + 'intermediateResults.txt'
        with open(fullPath,'r') as infile:
            inputJSON = infile.read()
        inputList = json.loads(inputJSON)
        
        # Initialize prior class probabilities (phi), centroids and covariance inverses
        self.phi = np.array(inputList[0])
        self.centroids = np.array(inputList[1])
        self.covInvs = np.array(inputList[2])
        
        # Initialize partial sums
        self.new_phi = np.zeros_like(self.phi)
        self.new_centroids = np.zeros_like(self.centroids)
        self.new_covInvs = np.zeros_like(self.covInvs)
        
        # Set number of mappers
        self.numMappers = 1
        
        # Initialize count
        self.count = 0
        
    # Configure job options
    def configure_options(self):
        super(GMM_EM_Iterate, self).configure_options()
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
            weightVector[i] = self.phi[i] * gaussProb(x, self.centroids[i], self.covInvs[i])

        weightVector /= sum(weightVector)
        
        #############################################
        # Maximization step part 1: 
        # Partial sums for equations (2), (3) and (4)
        #############################################
        
        self.count += 1
        
        # Equation (4)
        self.new_phi += weightVector
        
        # Equation (2) and (3)
        for i in range(k):
            
            # Partial sum for weighted xs (eq 2)
            self.new_centroids[i] += weightVector[i] * x
            
            # Partial sum for weighted squares (eq 3)
            xMinusMu = x - self.centroids[i]
            covInc = np.zeros_like(self.new_covInvs[i])
            
            for l in range(len(xMinusMu)):
                for m in range(len(xMinusMu)):
                    covInc[l][m] = xMinusMu[l] * xMinusMu[m]
                    
            self.new_covInvs[i] += weightVector[i] * covInc
        
    """
    Mapper Final
    - emit partially accumulated count, phi, centroids and covariance inverses
    """
    def mapper_final(self):
        out = [self.count, (self.new_phi).tolist(), (self.new_centroids).tolist(), (self.new_covInvs).tolist()]
        jOut = json.dumps(out)
        yield 1, jOut
    
    """
    Reducer
    - accumulate partial sums of count, phi, centroids and covariance inverses
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
                totalCovInvs = np.array(fields[3])
                first = False
            
            # If this isn't the first, accumulate
            else:
                fields = json.loads(val)
                
                totalCount += fields[0]
                totalPhi += np.array(fields[1])
                totalCentroids += np.array(fields[2])
                totalCovInvs += np.array(fields[3])
        
        # Finally divide
        # This finalizes equation (4)
        finalPhi = totalPhi / totalCount
        
        # Make a copy so that we know these are the right dimensions
        finalCentroids = np.array(totalCentroids)
        finalCov = np.array(totalCovInvs)
        
        for i in range(k):
            
            # This finalizes equation (2)
            finalCentroids[i, :] = totalCentroids[i, :] / totalPhi[i]
            finalCovInv = totalCovInvs[i, :, :] / totalPhi[i]
            
            # Now we can invert to get the actual covariance
            # This finalizes equation (3)
            finalCov[i, :, :] = np.linalg.inv(finalCovInv)
            
        # Create output
        outputList = [finalPhi.tolist(), finalCentroids.tolist(), finalCov.tolist()]
        jsonOut = json.dumps(outputList)
                
        # Write to file
        fullPath = self.options.pathName + 'intermediateResults.txt'
        with open(fullPath, 'w') as outfile:
            outfile.write(jsonOut)
            
    
if __name__ == '__main__':
    GMM_EM_Iterate.run()
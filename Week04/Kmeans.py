from numpy import argmin, array, random
from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import chain
import os

#Calculate find the nearest centroid for data point 
def MinDist(datapoint, centroid_points):
    datapoint = array(datapoint)
    centroid_points = array(centroid_points)
    diff = datapoint - centroid_points 
    diffsq = diff*diff
    # Get the nearest centroid for each instance
    minidx = argmin(list(diffsq.sum(axis = 1)))
    return minidx

#Check whether centroids converge
def stop_criterion(centroid_points_old, centroid_points_new, T):
    oldvalue = list(chain(*centroid_points_old))
    newvalue = list(chain(*centroid_points_new))
    Diff = [abs(x-y) for x, y in zip(oldvalue, newvalue)]
    Flag = True
    for i in Diff:
        if(i > T):
            Flag = False
            break
    return Flag


class MRKmeans(MRJob):
    
    centroid_points=[]
    k = 4
    n = 1000
    
    def steps(self):
        return [
            MRStep(mapper_init = self.mapper_init, 
                   mapper=self.mapper,
                   combiner = self.combiner,
                   reducer=self.reducer)
               ]
    
    #load centroids info from file
    def mapper_init(self):
        #os.chdir(r'/home/cloudera/Documents/W261-Fall2016/Week04')
        os.chdir(r'C:\Users\miki.seltzer\Google Drive\2016 01 Spring Semester\W261 - ML at Scale\W261-Fall2016\Week04')
        myfile = open('Centroids.txt','r')
        self.centroid_points = [map(float,s.split('\n')[0].split(',')) for s in myfile.readlines()]
        myfile.close()
        self.k = len(self.centroid_points)
    
    #load data and output the nearest centroid index and data point 
    def mapper(self, _, line):
        D = (map(float,line.split(',')))
        value = list(D)
        value.append(1)
        yield int(MinDist(D,self.centroid_points)), tuple(value)
    
    #Combine sum of data points locally
    def combiner(self, idx, inputdata):
        sums = [0 for i in range(self.n+1)]
        for point in inputdata:
            for i in range(self.n+1):
                sums[i] += point[i]
        yield idx, tuple(sums)
            
    #Aggregate sum for each cluster and then calculate the new centroids
    def reducer(self, idx, inputdata): 
        centroids = []
        num = [0]*self.k 
        for i in range(self.k):
            centroids.append([0 for i in range(self.n)])
        for point in inputdata:
            count = float(point[-1])
            num[idx] += count
            for i in range(self.n):       
                centroids[idx][i] += point[i]

        for i in range(len(centroids[idx])):
            centroids[idx][i] = centroids[idx][i]/num[idx]
        with open('Centroids.txt', 'a') as f:
            f.writelines(','.join(str(j) for j in centroids[idx]) + '\n')
        yield idx, tuple(centroids[idx])
      
if __name__ == '__main__':
    MRKmeans.run()
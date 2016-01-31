#!/usr/bin/python
## mapper.py
## Author: Miki Seltzer
## Description: mapper code for HW3.5a

import sys
import itertools

# Increment mapper counter
sys.stderr.write("reporter:counter:Custom_Counter,Mapper,1\n")

# Initialize variables
total = 0
stripes = {}

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    # Split our line into products
    products = line.replace('\n','').split()
    
    # Get all combinations of products:
    #  - Use a set to remove duplicate products
    #  - Combinations finds tuples of length 2 with no repeats
    items = sorted(list(set(products)))

    for i in range(len(items)-1):
        for j in range(i+1, len(items)):
            stripes[items[j]] = 1
        print '%s\t%s' % (items[i], stripes)
        stripes = {}
        
    # Increment total number of baskets
    total += 1
        
# Print total words
print '%s\t%s' % ('*total', {'*total':total})
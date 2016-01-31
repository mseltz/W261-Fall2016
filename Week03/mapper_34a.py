#!/usr/bin/python
## mapper.py
## Author: Miki Seltzer
## Description: mapper code for HW3.4a

import sys
import itertools

# Increment mapper counter
sys.stderr.write("reporter:counter:Custom_Counter,Mapper,1\n")

# Initialize variables
total = 0

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    # Split our line into products
    products = line.replace('\n','').split()
    
    # Get all combinations of products:
    #  - Use a set to remove duplicate products
    #  - Combinations finds tuples of length 2 with no repeats
    pairs = list(itertools.combinations(set(products), 2))
    
    # For each pair, sort the pair alphabetically, then emit
    for pair in pairs:
        sorted_pair = sorted(pair)
        print '%s\t%s\t%s' % (sorted_pair[0], sorted_pair[1], 1)
    
    # Increment total number of baskets
    total += 1
        
# Print total words
print '%s\t%s\t%s' % ('*total', '', total)
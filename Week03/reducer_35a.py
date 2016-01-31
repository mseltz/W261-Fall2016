#!/usr/bin/python
## reducer.py
## Author: Miki Seltzer
## Description: reducer code for HW3.5a

import sys
from operator import itemgetter

# Increment mapper counter
sys.stderr.write("reporter:counter:Custom_Counter,Reducer,1\n")

# Initialize variables
prev_word = None
prev_stripe = {}
total = 0

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    # Define our key and value
    fields = line.replace('\n','').split('\t')
    word = fields[0]
    stripe = eval(fields[1])

    # Check if we've moved to a new word
    if prev_word == word:
        # We need to move through the dictionary and update counts
        for item in stripe:
            if item in prev_stripe:
                prev_stripe[item] += stripe[item]
            else:
                prev_stripe[item] = stripe[item]
        
    else:
        if len(prev_stripe) > 0:
            # We are at a new pair, need to print previous pair sum
            print '%s\t%s' % (prev_word, prev_stripe)
        prev_stripe = stripe
        prev_word = word

# Output the last line
if prev_stripe == stripe:
    print '%s\t%s' % (prev_word, prev_stripe)
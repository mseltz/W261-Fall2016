#!/usr/bin/python
## reducer.py
## Author: Miki Seltzer
## Description: combiner code for HW3.4a

import sys
from operator import itemgetter

# Increment mapper counter
sys.stderr.write("reporter:counter:Custom_Counter,Combiner,1\n")

# Initialize variables
prev_pair = []
prev_count = 0
total = 0

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    # Define our key and value
    fields = line.replace('\n','').split('\t')
    pair = [fields[0], fields[1]]
    count = fields[2]

    # Convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # Count wasn't an int, so move on
        continue

    # Check if we've moved to a new word
    if prev_pair == pair:
        prev_count += count
    else:
        if len(prev_pair) > 0:
            # We are at a new pair, need to print previous pair sum
            print '%s\t%s\t%s' % (prev_pair[0], prev_pair[1], prev_count)
        prev_count = count
        prev_pair = pair

# Output the last line
if prev_pair == pair:
    print '%s\t%s\t%s' % (prev_pair[0], prev_pair[1], prev_count)
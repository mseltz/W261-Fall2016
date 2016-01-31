#!/usr/bin/python
## reducer.py
## Author: Miki Seltzer
## Description: reducer code for HW3.2b

import sys
from operator import itemgetter

# Increment mapper counter
sys.stderr.write("reporter:counter:Custom_Counter,Reducer,1\n")

# Initialize variables
prev_word = None
prev_count = 0

# Our input comes from STDIN (standard input)
for line in sys.stdin:

    # Split line
    word, count = line.split('\t')

    # Convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # Count wasn't an int, so move on
        continue

    if prev_word == word:
        # We haven't moved to a new word
        prev_count += count
    
    else:
        if prev_word:
            print '%s\t%s' % (prev_word, prev_count)

        prev_count = count
        prev_word = word

# Output the last line
if prev_word == word:
    print '%s\t%s' % (prev_word, prev_count)    
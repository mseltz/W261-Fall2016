#!/usr/bin/python
## combiner.py
## Author: Miki Seltzer
## Description: combiner code for HW3.2c

import sys
from operator import itemgetter

# Increment mapper counter
sys.stderr.write("reporter:counter:Custom_Counter,Combiner,1\n")

prev_word = None
prev_count = 0

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    word, count = line.split('\t')

    # Convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # Count wasn't an int, so move on
        continue

    # Check if we've moved to a new word
    if prev_word == word:
        prev_count += count
    else:
        if prev_word:
            # We are at a new word, need to print previous word sum
            print '%s\t%s' % (prev_word, prev_count)
        prev_count = count
        prev_word = word

# Output the last line
if prev_word == word:
    print '%s\t%s' % (prev_word, prev_count)
    
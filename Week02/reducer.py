#!/usr/bin/python
## reducer.py
## Author: Miki Seltzer
## Description: reducer code for HW2.2

from operator import itemgetter
import sys

# Initialize some variables
# We know that the words will be sorted
# We need to keep track of state
prev_word = None
prev_count = 0
word = None

for line in sys.stdin:
    # Strip and split line from mapper
    word, count = line.strip().split('\t', 1)
    
    # If possible, turn count into an int (it's read as a string)
    try:
        count = int(count)
    except ValueError:
        # We couldn't make count into an int, so move on
        continue
        
    # Since the words will be sorted, all counts for a word will be grouped
    if prev_word == word:
        # If prev_word is word, then we haven't changed words
        # Just update prev_count
        prev_count += count
    else:
        # We've encountered a new word!
        # This might be the first word, though
        if prev_word:
            # We need to print the last word we were on
            print "%s\t%s" % (prev_word, prev_count)
        
        # Now we need to initialize our variables for the new word and count
        prev_word = word
        prev_count = count

# We have reached the end of the file, so print the last word and count
if prev_word == word:
    print "%s\t%s" % (prev_word, prev_count)
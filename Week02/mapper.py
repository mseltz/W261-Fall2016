#!/usr/bin/python
## mapper.py
## Author: Miki Seltzer
## Description: mapper code for HW2.2

import sys

# Get the user-specified word
keyword = sys.argv[1]

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    
    # Strip white space from line, then split
    words = line.strip().split()
    
    # Loop through words
    # If it matches the keyword, write it to file
    # key = word
    # value = 1
    for word in words:
        if word == keyword:
            print "%s\t%s" % (word, 1)
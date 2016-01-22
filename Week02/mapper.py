#!/usr/bin/python
## mapper.py
## Author: Miki Seltzer
## Description: mapper code for HW2.2

import sys
import string

# Get the user-specified word
keyword = sys.argv[1]

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    
    # Strip white space from line, then split
    # Some records are malformed -- if there is a 4th field, use it
    fields = line.strip().split('\t')
    subj = fields[2].translate(string.maketrans("",""), string.punctuation)
    if len(fields) == 4:
        body = fields[3].translate(string.maketrans("",""), string.punctuation)
    else:
        body = ""
    words = subj + " " + body
    words = words.split()
    
    # Loop through words
    # If it matches the keyword, write it to file
    # key = word
    # value = 1
    for word in words:
        if word == keyword:
            print "%s\t%s" % (word, 1)
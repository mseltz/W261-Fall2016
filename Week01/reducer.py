#!/usr/bin/python
## reducer.py
## Author: Miki Seltzer
## Description: reducer code for HW1.2

import sys
sum = 0

filenames = sys.argv[1:]

wordcount = {}

# Each mapper outputs a [word, count] pair
# For each file and each word, sum the counts
for file in filenames:
    with open(file, "rU") as myfile:
        for line in myfile:
            pair = eval(line)
            word = pair[0]
            count = pair[1]
            if word not in wordcount:
                wordcount[word] = 0
            wordcount[word] += count
            
for word in wordcount:
    print word, "\t", wordcount[word]
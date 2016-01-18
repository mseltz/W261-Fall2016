#!/usr/bin/python
## mapper.py
## Author: Miki Seltzer
## Description: mapper code for HW1.2

import sys
import re
count = 0

# Collect input
filename = sys.argv[1]
findwords = re.split(" ",sys.argv[2].lower())

# Initialize dictionary to empty
wordcount = {}

with open(filename, "rU") as myfile:
    for line in myfile:
        # Format each line, fields separated by \t according to enronemail_README.txt
        fields = re.split("\t", line)
        
        # For each word in list provided by user, count occurrences in subj and body
        for word in findwords:
            if word not in wordcount:
                wordcount[word] = 0 
            wordcount[word] += fields[2].count(word) + fields[3].count(word)

for word in wordcount:
    print [word, wordcount[word]]
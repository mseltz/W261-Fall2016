#!/usr/bin/python
## mapper.py
## Author: Miki Seltzer
## Description: mapper code for HW1.5

import sys
import re
import string

# Collect input
filename = sys.argv[1]
findwords = re.split(" ",sys.argv[2].lower())

# Initialize dictionary to empty
word_count = {}

with open(filename, "rU") as myfile:
    for line in myfile:
        # Format each line, fields separated by \t according to enronemail_README.txt
        fields = re.split("\t", line)
        fields[3] = fields[3].replace("\n", "")
        subj = fields[2].translate(string.maketrans("",""), string.punctuation)
        body = fields[3].translate(string.maketrans("",""), string.punctuation)
        
        # For each word, count occurrences in subj and body
        # Key is (document id, spam, word)
        if findwords[0] == "*":
            full_text = subj + " " + body
            for word in full_text.split():
                my_key = (fields[0], fields[1], word)
                if my_key not in word_count:
                    word_count[my_key] = 0.0
                word_count[my_key] += 1
        else:
            for word in findwords:
                my_key = (fields[0], fields[1], word)
                if my_key not in word_count:
                    word_count[my_key] = 0.0
                word_count[my_key] += subj.count(word) + body.count(word)

for key in word_count:
    print [key, word_count[key]]
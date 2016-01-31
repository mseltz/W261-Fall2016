#!/usr/bin/python
## mapper.py
## Author: Miki Seltzer
## Description: mapper code for HW3.1

import sys
from csv import reader

# Our input comes from STDIN (standard input)
for line in reader(sys.stdin):
    product = line[1]
    if product == "Debt collection": sys.stderr.write("reporter:counter:Product,Debt,1\n")
    elif product == "Mortgage": sys.stderr.write("reporter:counter:Product,Mortgage,1\n")
    else: sys.stderr.write("reporter:counter:Product,Other,1\n")
    print line
    
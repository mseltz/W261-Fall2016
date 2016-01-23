#!/usr/bin/python
## reducer.py
## Author: Miki Seltzer
## Description: reducer code for HW2.3

from operator import itemgetter
import sys
import math
import re

# Initialize some variables
prev_doc = None
prev_spam = None
prev_spam_count = 0
prev_ham_count = 0
prev_word = None

docs_total = 0
docs = {'1':0, '0':0}
words_total = 0
words = {'1':0, '0':0}
log_prior = {'1':0, '0':0}
log_posterior = {'1':0, '0':0}
log_likelihood = {'1':0, '0':0}
log_evidence = 0

num_errors = 0.0
num_total = 0.0
num_correct = 0.0

for line in sys.stdin:
    # Strip and split line
    # Assign variables
    line = line.replace('\n', '')
    key, value = line.strip().split('\t')
    doc, spam, word = key.split('^')
    spam_count, ham_count = value.split('^')
    
    try:
        spam_count = float(spam_count)
        ham_count = float(ham_count)
    except ValueError:
        continue

    # Let's calculate some log_probs!
    if prev_doc == doc:
        # We haven't changed documents
        if prev_word == word:
            # We haven't changed words, so just increment
            prev_spam_count += spam_count
            prev_ham_count += ham_count
            #print "update counts"
            
        else:
            # We are at a new word
            # We need to check if we are at a keyword
            if prev_word == '*alldocs': 
                # We are at a record where we need to output total docs
                docs_total = prev_spam_count
                #print "total docs:", docs_total
            
            elif prev_word == '*docs': 
                # We are at a record where we need to output unique docs per class
                docs['1'] = prev_spam_count
                docs['0'] = prev_ham_count
                for item in log_prior:
                    log_prior[item] = math.log(docs[item] / docs_total)
                    
                    # We will update the posterior after each word
                    # Initialize it to the prior
                    log_posterior[item] = math.log(docs[item] / docs_total)
                #print "log prior:", log_prior

            elif prev_word == '*words':
                # We are at a record where we need to output words per class
                words['1'] = prev_spam_count
                words['0'] = prev_ham_count
                words_total = prev_spam_count + prev_ham_count
                #print "word count:", words

            elif prev_word:
                # We are at a new normal word, and need to calculate stuff
                try:
                    log_likelihood['1'] = math.log(prev_spam_count / words['1'])
                    #print word, "log likelihood spam for", word, log_likelihood['1']
                    
                except:
                    #print "error calculating log likelihood spam for", word
                    num_errors += 1
                    
                try:
                    log_likelihood['0'] = math.log(prev_ham_count / words['0'])
                    #print word, "log likelihood ham for", word, log_likelihood['0']
                    
                except:
                    #print "error calculating log likelihood ham for", word
                    num_errors += 1
                    
                # Calculate evidence
                log_evidence = math.log((prev_spam_count + prev_ham_count)/words_total)
                for item in log_posterior:
                    log_posterior[item] += log_likelihood[item] - log_evidence
                #print "updated log posterior:", log_posterior
                
                
            prev_word = word
            prev_spam_count = spam_count
            prev_ham_count = ham_count
            
    else:
        # We are done with one document, and need to output our predictions
        if prev_doc:
            # We know that we're not on the very first record
            if log_posterior['1'] > log_posterior['0']: prediction = '1'
            else: prediction = '0'
            num_total += 1
            if prev_spam == prediction: num_correct += 1
            print '%s\t%s\t%s\t%s\t%s' % (prev_doc, prev_spam, prediction, 
                                          log_posterior['1'],
                                          log_posterior['0'])
        
        prev_doc = doc
        prev_spam = spam
        prev_word = word
        prev_spam_count = spam_count
        prev_ham_count = ham_count

# Output our final prediction
if log_posterior['1'] > log_posterior['0']: prediction = '1'
else: prediction = '0'
num_total += 1
if prev_spam == prediction: num_correct += 1
print '%s\t%s\t%s\t%s\t%s' % (prev_doc, prev_spam, prediction, 
                              log_posterior['1'],
                              log_posterior['0'])
print '\n'
print "Number of documents:", num_total
print "Number correct predictions:", num_correct
print "Accuracy:", num_correct / num_total
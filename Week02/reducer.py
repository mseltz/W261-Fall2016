#!/usr/bin/python
## reducer.py
## Author: Miki Seltzer
## Description: reducer code for HW2.3

from operator import itemgetter
import sys
import math
import re
import decimal

# Initialize some variables
doc = None
spam = None
count = 1
class_count = {'1':0, '0':0}
word = None

prev_doc = None
prev_spam = None
prev_count = 1
prev_class_count = {'1':0, '0':0}
prev_word = None

docs_total = 0
docs = {'1':0, '0':0}
words_total = 0
words = {'1':0, '0':0}
log_prior = {'1':0, '0':0}
log_posterior = {'1':0, '0':0}
log_likelihood = {'1':0, '0':0}

classes = {'1':'spam', '0':'ham'}
num_errors = {'1':0, '0':0}
num_total = 0.0
num_correct = 0.0

print_debug = False

# Create a function to update the posterior
# since we need to do it in multiple locations.
# We don't want to duplicate code
def update_posterior():
    # Calculate evidence
    if print_debug:
        print "times word occured:", prev_count

    for item in classes:
        if prev_class_count[item] > 0 and log_likelihood[item] != float('-inf'):
            log_likelihood[item] = math.log(prev_class_count[item] / words[item])
            log_posterior[item] += prev_count * log_likelihood[item]
            if print_debug:
                print "updated log posterior:", log_posterior[item]

        else:
            if print_debug:
                print "zero probability, set log_post to -inf for", classes[item]
                print '\n'
            log_posterior[item] = float('-inf')
            num_errors[item] += 1
        

# Create a function to avoid code duplication
def make_prediction(): 
    global num_total, num_correct
    
    # We can compare non-normalized posterior probabilities
    num_total += 1
    if log_posterior['1'] > log_posterior['0']: prediction = '1'
    else: prediction = '0'

    # Count correct guesses
    if prev_spam == prediction:
        num_correct += 1
        
    # Output the log posteriors. We can normalize later.
    print '%s\t%s\t%s\t%s\t%s\n' % (prev_doc, prev_spam, prediction, 
                                    log_posterior['1'],
                                    log_posterior['0'])
    
for line in sys.stdin:
    # Remove end of line
    line = line.replace('\n', '')
    
    # Split words when running locally
    #doc, spam, word, class_count['1'], class_count['0'] = line.split('\t')
    
    # Split words when running in Hadoop
    key, value = line.strip().split('\t')
    doc, spam, word = key.split('^')
    class_count['1'], class_count['0'] = value.split('^')    

    # Keep this in a try/except statement so we don't fail
    try:
        for item in classes:
            class_count[item] = float(class_count[item])
    except ValueError:
        continue

    # Let's calculate some probabilities
    if prev_doc == doc:
        # We haven't changed documents
        if prev_word == word:
            # We haven't changed words, so just increment
            prev_count += 1

        else:
            # We are at a new word
            # We need to check if we are at a keyword
            if print_debug: print '\n', prev_word, '\n'
            if prev_word == '*alldocs': 
                # We are at a record where we need to output total docs
                docs_total = prev_class_count['1']
                if print_debug: print "total docs:", docs_total

            elif prev_word == '*docs': 
                # We are at a record where we need to output unique docs per class
                for item in classes:
                    docs[item] = prev_class_count[item]
                    if print_debug: print "prior", item, docs[item], '/', docs_total
                    log_prior[item] = math.log(docs[item] / docs_total)

                    # We will update the posterior after each word
                    # Initialize it to the prior
                    log_posterior[item] = log_prior[item]
                if print_debug: 
                    print "log prior:", log_prior
                    print 'log posterior initial', log_posterior

            elif prev_word == '*words':
                # We are at a record where we need to output words per class
                for item in classes:
                    words[item] = prev_class_count[item]
                words_total = sum(prev_class_count.values())
                if print_debug: print "word class_count:", words

            elif prev_word:
                # We are at a new normal word, and need to calculate stuff
                update_posterior()

            prev_word = word
            prev_count = 1
            for item in classes:
                prev_class_count[item] = class_count[item]

    else:
        # We are done with one document. We need to: 
        # - process the last word
        # - output our predictions
        if prev_doc:
            if print_debug: print '\n', prev_word, '\n'
            # We are at a new normal word, and need to calculate stuff
            update_posterior()

            # Now we can calculate the prediction
            make_prediction()
            if print_debug: print num_correct, "out of", num_total

        prev_doc = doc
        prev_spam = spam
        prev_word = word
        for item in classes:
            prev_class_count[item] = class_count[item]
        log_likelihood = {'1':0, '0':0}
        if print_debug: print "reset log likelihood"

# Output our final prediction
if print_debug: print '\n', prev_word, '\n'
update_posterior()
make_prediction()

print "Number of documents\t%d" % (num_total)
print "Number correct predictions\t%d" % (num_correct)
print "Error rate\t%s" % (100 - 100 * num_correct / num_total) + "%"
print "Number of zero probability spam\t%d" % (num_errors['1'])
print "Number of zero probability ham\t%d" % (num_errors['0'])
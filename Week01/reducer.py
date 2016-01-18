#!/usr/bin/python
## reducer.py
## Author: Miki Seltzer
## Description: reducer code for HW1.3

import sys
import math

filenames = sys.argv[1:]

doc_ids = {}
document_words = {}
class_counts = {'1':0.0, '0':0.0}
vocab = {}
word_counts = {
    '0': {},
    '1': {}
}

for file in filenames:
    # Train classifier with data from mapper.py
    with open(file, "r") as myfile:
        for line in myfile:
            pair = eval(line)
            doc_id = pair[0][0]
            spam = pair[0][1]
            word = pair[0][2]
            count = int(pair[1])
            
            # We need to aggregate counts on specific levels. We need:
            #    - number of total documents
            #    - number of documents per class
            #    - our entire vocabulary
            #    - word counts per class
            #    - word counts per document (for testing purposes)
            if doc_id not in doc_ids: 
                doc_ids[doc_id] = spam
                class_counts[spam] += 1
                document_words[doc_id] = {}
            if word not in vocab: vocab[word] = 0.0
            vocab[word] += count
            if word not in word_counts[spam]: word_counts[spam][word] = 0.0
            word_counts[spam][word] += count
            if word not in document_words[doc_id]: document_words[doc_id][word] = 0.0
            document_words[doc_id][word] += count
            

prior_spam = class_counts['1'] / len(doc_ids)
prior_ham = class_counts['0'] / len(doc_ids)

for doc in document_words:
    pred = 'none'

    log_prob_spam_word = math.log(prior_spam)
    log_prob_ham_word = math.log(prior_ham)
    
    # Test classifier using training data
    for word in document_words[doc]:


        # Make sure that the word was in the training data
        if word not in vocab: continue

        # Calculate P(word)
        p_word = vocab[word] / sum(vocab.values())

        # Calculate P(word|spam) and P(word|ham)
        # Use add-1 smoothing
        if word not in word_counts['1']: num_word_spam = 0
        else: num_word_spam = word_counts['1'][word]
        p_word_spam = (num_word_spam + 1.0) / (sum(word_counts['1'].values()) + len(vocab))

        if word not in word_counts['0']: num_word_ham = 0
        else: num_word_ham = word_counts['0'][word]
        p_word_ham = (num_word_ham + 1.0) / (sum(word_counts['0'].values()) + len(vocab))

        # Update probabilities
        log_prob_spam_word += math.log(p_word_spam) * document_words[doc][word]
        log_prob_ham_word += math.log(p_word_ham) * document_words[doc][word]

    if log_prob_spam_word > log_prob_ham_word: pred = '1'
    else: pred = '0'

    print doc + "\t" + doc_ids[doc] + "\t" + pred
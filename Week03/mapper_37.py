#!/usr/bin/env python
import itertools
import sys

item_count = int(sys.argv[1])
valid_items = set()

# If our item count is greater than 1, then load the corresponding model file
# indicating the items we should care about.

if item_count > 1:
    model_id = str(item_count - 1)

    # The first k items in each model row will correspond to the products. We
    # can build up the set of valid items simply by iterating over the model
    # and adding each of the elements in the first k columns.

    with open('apriori_model_' + model_id + '.txt') as model_file:
        for line in model_file:
            model_row = line.strip().split('\t')
            old_itemset = model_row[0:item_count - 1]
            valid_items.update(old_itemset)

"""
Emit all the itemsets for this basket.
"""
def emit_itemsets(basket):
    # First, we need to find out which items in the basket match the items
    # which we can accept in our k-itemsets. Note that we will accept every
    # item when the item count is 1.
    
    matching_items = []
    
    for item in basket:
        if item_count == 1 or item in valid_items:
            matching_items.append(item)
    
    # If we don't have enough items, we have no itemsets to emit.
    
    if len(matching_items) < item_count:
        return

    # Otherwise, emit all possible subsets. We'll use the pairs approach to
    # make things easier to read.
    
    for itemset in itertools.permutations(matching_items, item_count):
        print '\t'.join(itemset) + '\t1'
        
    # Also emit a counter for subcombinations so that we can create a
    # tally to use for computing confidence.

    if item_count > 1:
        for sub_itemset in itertools.permutations(matching_items, item_count - 1):
            print '\t'.join(sub_itemset) + '\t*\t1'
        
    # Finally, counter so that we can track the number of matching baskets.

    print ('*\t' * item_count) + str(1)
    
# Iterate over the baskets and emit the itemsets for each basket.

for line in sys.stdin:
    basket = line.strip().split(' ')
    emit_itemsets(basket)
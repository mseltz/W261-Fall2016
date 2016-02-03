#!/usr/bin/env python
import sys

support_threshold = int(sys.argv[1])

basket_count = 0
confidence_count = 0

current_itemset = None
current_count = 0

"""
Emit the current itemset and its count if they exceed support_threshold.
"""
def emit_count():

    # Declare that we want to use the global basket_count and confidence_count
    # variables rather than something local to the function.
    
    global basket_count, confidence_count
    
    # Check if we haven't started counting anything yet.
    
    if current_itemset is None:
        return

    # Check if we are computing the basket count from the sort operation.
    
    if current_itemset[0] == '*':
        basket_count = current_count
        return

    if current_itemset[-1] == '*':
        confidence_count = current_count
        return

    # Check if we have exceeded the necessary threshold.

    if current_count >= support_threshold:
        frequency = 1.0 * current_count / basket_count
        
        itemset_stats = str(current_count) + '\t' + str(frequency)
        
        if len(current_itemset) > 1:
            confidence = 1.0 * current_count / confidence_count
            itemset_stats += '\t' + str(confidence)
        
        print '\t'.join(current_itemset) + '\t' + itemset_stats

for line in sys.stdin:

    # Each line corresponds to the itemset stats. The last item will be a count
    # value, while the first items will be the itemset.
    
    itemset_stats = line.strip().split('\t')

    itemset = itemset_stats[0:-1]
    count = int(itemset_stats[-1])

    # If we haven't switched itemsets, continue accumulating the counter.
    
    if current_itemset == itemset:
        current_count += count
        continue

    # If we have switched itemsets, emit the count for the old itemset and then
    # switch to the new itemset.
        
    emit_count()
    current_itemset = itemset
    current_count = count

# We are guaranteed to not have printed the very last itemset, so emit it now.
    
emit_count()
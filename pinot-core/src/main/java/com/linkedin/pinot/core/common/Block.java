package com.linkedin.pinot.core.common;

/**
 * 
 * A block represents a set of rows.A segment will contain one or more blocks
 * Currently, it assumes only one column per block. We might change this in
 * future
 */
public interface Block {

    /**
     * returns the blockid, a segment will
     * 
     * @return
     */
    BlockId getId();

    /**
     * Allows one to push down the predicates
     * 
     * @param predicate
     * @return
     */
    boolean applyPredicate(Predicate predicate);

    /**
     * Returns valset that allows one to iterate over the docId. If no predicate
     * is provided, this will consists of all docIds within the block
     * 
     * @return {@link BlockDocIdSet}
     */

    BlockDocIdSet getBlockDocIdSet();

    /**
     * Returns valset that allows one to iterate over the values
     * 
     * @return {@link BlockValSet}
     */
    BlockValSet getBlockValueSet();

    /**
     * Allows one to iterate over the DocId And Value in parallel
     * 
     * @return
     */
    BlockDocIdValueSet getBlockDocIdValueSet();

    /**
     * For future optimizations. The metadata can consists of bloom filter,
     * min/max, sum, count etc that can be used in filtering, aggregation
     * 
     * @return
     */
    BlockMetadata getMetadata();


}

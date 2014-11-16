package com.linkedin.pinot.core.common;

/**
 * 
 * Not used for now, since most of the predicates make use of inverted index.
 * Revisit when we push predicate evaluation up into the operator
 */
public interface BlockDocIdValueIterator {

	boolean advance();

	int currentDocId();

	int currentVal();
}

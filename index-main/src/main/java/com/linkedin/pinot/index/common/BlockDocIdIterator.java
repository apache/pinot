package com.linkedin.pinot.index.common;

public interface BlockDocIdIterator {
	/**
	 * returns the currentDocId the iterator is currently pointing to, -1 if
	 * next/advance is not yet called, EOF if the iteration has exhausted
	 * 
	 * @return
	 */
	int currentDocId();

	/**
	 * advances to next document in the set and returns the nextDocId, EOF if
	 * there are no more docs
	 * 
	 * @return
	 */
	int next();

	/**
	 * skips to first entry beyond current docId whose docId is equal or greater
	 * than targetDocId
	 * 
	 * @param docId
	 * @return
	 */
	int skipTo(int targetDocId);

}

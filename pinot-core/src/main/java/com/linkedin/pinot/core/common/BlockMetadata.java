package com.linkedin.pinot.core.common;

public interface BlockMetadata {

	int getSize();

	//additional info about the docIdSet
	int getLength();
	
	int getStartDocId();
	
	int getEndDocId();
	
	//DocId set properties
	
	boolean isSorted();
	
	boolean isSparse();

	boolean hasInvertedIndex();
	
	//boolean getForwardIndexCompressionType();
	
   //boolean getInvertedIndexCompressionType();

}

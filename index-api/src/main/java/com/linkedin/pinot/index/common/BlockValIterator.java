package com.linkedin.pinot.index.common;

public interface BlockValIterator {
	
	int nextVal();
		
	int currentDocId();
	
	int currentValId();

	boolean reset();
}

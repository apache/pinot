package com.linkedin.pinot.core.common;

public interface BlockValIterator {
	
	int nextVal();
		
	int currentDocId();
	
	int currentValId();

	boolean reset();
}

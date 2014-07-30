package com.linkedin.pinot.core.common;

public interface  BlockBuilder<T> {

	void addDoc(int docId);
	
	Block build();
}

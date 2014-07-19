package com.linkedin.pinot.index.common;

public interface  BlockBuilder<T> {

	void addDoc(int docId);
	
	Block build();
}

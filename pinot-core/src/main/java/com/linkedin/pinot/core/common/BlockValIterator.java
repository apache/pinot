package com.linkedin.pinot.core.common;

import com.linkedin.pinot.common.data.FieldSpec.DataType;

/**
 * 
 *
 */
public interface BlockValIterator {

	boolean skipTo(int docId);

	int currentDocId();

	boolean reset();

	boolean next();

	/**
	 * Use it only when you want to check if there is next but dont want to move
	 * the cursor. If you plan to move the cursor, use next for better performance
	 * 
	 * @return
	 */
	boolean hasNext();

	int size();

	DataType getValueType();
}

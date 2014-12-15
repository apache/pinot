package com.linkedin.pinot.core.index.reader;

public interface SingleColumnSingleValueReader extends DataFileReader{

	/**
	 * fetch the char at a row
	 * 
	 * @param row
	 * @return
	 */
	char getChar(int row);

	/**
	 * fetch short value at a specific row, col
	 * 
	 * @param row
	 * @return
	 */
	short getShort(int row);

	/**
	 * 
	 * @param row
	 * @return
	 */
	int getInt(int row);

	/**
	 * 
	 * @param row
	 * @return
	 */
	long getLong(int row);

	/**
	 * 
	 * @param row
	 * @return
	 */
	float getFloat(int row);

	/**
	 * 
	 * @param row
	 * @return
	 */
	double getDouble(int row);

	/**
	 * 
	 * @param row
	 * @return
	 */
	String getString(int row);

	/**
	 * 
	 * @param row
	 * @return
	 */
	byte[] getBytes(int row);
}

package com.linkedin.pinot.core.index.reader;

public interface MultiColumnSingleValueReader extends DataFileReader{
	/**
	 * fetch the char at a row,col
	 * 
	 * @param row
	 * @param col
	 * @return
	 */
	char getChar(int row, int col);

	/**
	 * fetch short value at a specific row, col
	 * 
	 * @param row
	 * @param col
	 * @return
	 */
	short getShort(int row, int col);

	/**
	 * 
	 * @param row
	 * @param col
	 * @return
	 */
	int getInt(int row, int col);

	/**
	 * 
	 * @param row
	 * @param col
	 * @return
	 */
	long getLong(int row, int col);

	/**
	 * 
	 * @param row
	 * @param col
	 * @return
	 */
	float getFloat(int row, int col);

	/**
	 * 
	 * @param row
	 * @param col
	 * @return
	 */
	double getDouble(int row, int col);

	/**
	 * 
	 * @param row
	 * @param col
	 * @return
	 */
	String getString(int row, int col);

	/**
	 * 
	 * @param row
	 * @param col
	 * @return
	 */
	byte[] getBytes(int row, int col);

}

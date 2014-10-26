package com.linkedin.pinot.core.index.reader;

public interface SingleColumnMultiValueReader extends DataFileReader {

	/**
	 * Read the multiple values for a column at a specific row.
	 * 
	 * @param row
	 * @param charArray
	 * @return returns the number of chars read
	 */
	int getCharArray(int row, char[] charArray);

	/**
	 * 
	 * @param row
	 * @param col
	 * @param shortsArray
	 * @return return the number of shorts read
	 */
	int getShortArray(int row, short[] shortsArray);

	/**
	 * 
	 * @param row
	 * @param col
	 * @param intArray
	 * @return
	 */
	int getIntArray(int row, int[] intArray);

	/**
	 * 
	 * @param row
	 * @param col
	 * @param longArray
	 * @return
	 */
	int getLongArray(int row, long[] longArray);

	/**
	 * 
	 * @param row
	 * @param col
	 * @param floatArray
	 * @return
	 */
	int getFloatArray(int row, float[] floatArray);

	/**
	 * 
	 * @param row
	 * @param col
	 * @param doubleArray
	 * @return
	 */
	int getDoubleArray(int row, double[] doubleArray);

	/**
	 * 
	 * @param row
	 * @param col
	 * @param stringArray
	 * @return
	 */
	int getStringArray(int row, String[] stringArray);

	/**
	 * 
	 * @param row
	 * @param col
	 * @param bytesArray
	 * @return
	 */
	int getBytesArray(int row, byte[][] bytesArray);
}

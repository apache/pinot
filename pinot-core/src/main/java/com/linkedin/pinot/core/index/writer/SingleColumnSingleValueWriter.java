package com.linkedin.pinot.core.index.writer;

public interface SingleColumnSingleValueWriter extends DataFileWriter {
	/**
	 * 
	 * @param row
	 * @param ch
	 */
	public void setChar(int row, char ch);

	/**
	 * 
	 * @param row
	 * @param i
	 */
	public void setInt(int row, int i);

	/**
	 * 
	 * @param row
	 * @param s
	 */
	public void setShort(int row, short s);

	/**
	 * 
	 * @param row
	 * @param l
	 */
	public void setLong(int row, int l);

	/**
	 * 
	 * @param row
	 * @param f
	 */
	public void setFloat(int row, float f);

	/**
	 * 
	 * @param row
	 * @param d
	 */
	public void setDouble(int row, double d);

	/**
	 * 
	 * @param row
	 * @param string
	 */
	public void setString(int row, String string) throws Exception;

	/**
	 * 
	 * @param row
	 * @param bytes
	 */
	public void setBytes(int row, byte[] bytes);

}

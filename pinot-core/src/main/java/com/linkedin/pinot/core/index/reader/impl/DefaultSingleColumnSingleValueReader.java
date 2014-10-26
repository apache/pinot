package com.linkedin.pinot.core.index.reader.impl;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.core.index.reader.DataFileMetadata;
import com.linkedin.pinot.core.index.reader.SingleColumnSingleValueReader;

/**
 * 
 * @author kgopalak
 * 
 */
public class DefaultSingleColumnSingleValueReader implements
		SingleColumnSingleValueReader {

	private FixedByteWidthRowColDataFileReader reader;

	public DefaultSingleColumnSingleValueReader(File file, int rows,
			int[] columnSizes, boolean isMMap) throws IOException {
		if (isMMap) {
			reader = FixedByteWidthRowColDataFileReader.forMmap(file, rows, 1,
					columnSizes);
		} else {
			reader = FixedByteWidthRowColDataFileReader.forHeap(file, rows, 1,
					columnSizes);
		}
	}

	@Override
	public char getChar(int row) {
		return reader.getChar(row, 0);
	}

	@Override
	public short getShort(int row) {
		return reader.getShort(row, 0);
	}

	@Override
	public int getInt(int row) {
		return reader.getInt(row, 0);
	}

	@Override
	public long getLong(int row) {
		return reader.getLong(row, 0);

	}

	@Override
	public float getFloat(int row) {
		return reader.getFloat(row, 0);
	}

	@Override
	public double getDouble(int row) {
		return reader.getDouble(row, 0);
	}

	@Override
	public String getString(int row) {
		return reader.getString(row, 0);
	}

	@Override
	public byte[] getBytes(int row) {
		return reader.getBytes(row, 0);
	}

	@Override
	public boolean open() {
		return reader.open();
	}

	@Override
	public DataFileMetadata getMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean close() {
		return reader.close();
	}
}

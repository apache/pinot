package com.linkedin.pinot.core.index.writer.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.linkedin.pinot.core.index.reader.DataFileMetadata;
import com.linkedin.pinot.core.index.reader.impl.FixedByteWidthRowColDataFileReader;
import com.linkedin.pinot.core.index.writer.SingleColumnMultiValueWriter;

public class FixedBitWidthSingleColumnMultiValueWriter implements
		SingleColumnMultiValueWriter {
	private static int SIZE_OF_INT = 4;
	private static int NUM_COLS_IN_HEADER = 2;
	private ByteBuffer headerBuffer;
	private ByteBuffer dataBuffer;
	private RandomAccessFile raf;
	private FixedByteWidthRowColDataFileWriter headerWriter;
	private FixedBitWidthRowColDataFileWriter dataWriter;
	private FixedByteWidthRowColDataFileReader headerReader;

	public FixedBitWidthSingleColumnMultiValueWriter(File file, int numDocs,
			int totalNumValues, int columnSizeInBits) throws Exception {
		// there will be two sections header and data
		// header will contain N lines, each line corresponding to the
		int headerSize = numDocs * SIZE_OF_INT * NUM_COLS_IN_HEADER;
		int dataSize = (totalNumValues * columnSizeInBits +7)/8;
		int totalSize = headerSize + dataSize;
		raf = new RandomAccessFile(file, "rw");
		headerBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0,
				headerSize);
		dataBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE,
				headerSize, dataSize);
		headerWriter = new FixedByteWidthRowColDataFileWriter(headerBuffer,
				numDocs, 2, new int[] { SIZE_OF_INT, SIZE_OF_INT });
		headerReader = new FixedByteWidthRowColDataFileReader(headerBuffer,
				numDocs, 2, new int[] { SIZE_OF_INT, SIZE_OF_INT });

		dataWriter = new FixedBitWidthRowColDataFileWriter(dataBuffer,
				totalNumValues, 1, new int[] { columnSizeInBits });

	}

	@Override
	public boolean open() {
		return true;
	}

	@Override
	public boolean setMetadata(DataFileMetadata metadata) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean close() {
		if (raf != null) {
			try {
				raf.close();
			} catch (IOException e) {
				return false;
			}
		}
		return false;
	}

	private int updateHeader(int row, int length) {
		int prevRowStartIndex = 0;
		int prevRowLength = 0;
		if (row > 0) {
			prevRowStartIndex = headerReader.getInt(row - 1, 0);
			prevRowLength = headerReader.getInt(row - 1, 1);
		}
		int newStartIndex = prevRowStartIndex + prevRowLength;
		headerWriter.setInt(row, 0, newStartIndex);
		headerWriter.setInt(row, 1, length);
		return newStartIndex;
	}

	@Override
	public void setCharArray(int row, char[] charArray) {
		throw new UnsupportedOperationException(
				"Only int data type is supported in fixedbit format");

	}

	@Override
	public void setShortArray(int row, short[] shortsArray) {
		throw new UnsupportedOperationException(
				"Only int data type is supported in fixedbit format");

	}

	@Override
	public void setIntArray(int row, int[] intArray) {
		int newStartIndex = updateHeader(row, intArray.length);
		for (int i = 0; i < intArray.length; i++) {
			dataWriter.setInt(newStartIndex + i, 0, intArray[i]);
		}
	}

	@Override
	public void setLongArray(int row, long[] longArray) {
		throw new UnsupportedOperationException(
				"Only int data type is supported in fixedbit format");

	}

	@Override
	public void setFloatArray(int row, float[] floatArray) {
		throw new UnsupportedOperationException(
				"Only int data type is supported in fixedbit format");

	}

	@Override
	public void setDoubleArray(int row, double[] doubleArray) {
		throw new UnsupportedOperationException(
				"Only int data type is supported in fixedbit format");

	}

	@Override
	public void setStringArray(int row, String[] stringArray) {
		throw new UnsupportedOperationException(
				"Only int data type is supported in fixedbit format");

	}

	@Override
	public void setBytesArray(int row, byte[][] bytesArray) {
		throw new UnsupportedOperationException(
				"Only int data type is supported in fixedbit format");
	}

}

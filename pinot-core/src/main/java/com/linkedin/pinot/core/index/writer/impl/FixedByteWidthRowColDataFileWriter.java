package com.linkedin.pinot.core.index.writer.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

public class FixedByteWidthRowColDataFileWriter {
	private File file;
	private int cols;
	private int[] columnOffsets;
	private int rows;
	private ByteBuffer byteBuffer;
	private RandomAccessFile raf;
	private int rowSizeInBytes;

	public FixedByteWidthRowColDataFileWriter(File file, int rows, int cols,
			int[] columnSizes) throws Exception {
		this.file = file;
		this.rows = rows;
		this.cols = cols;
		this.columnOffsets = new int[cols];
		raf = new RandomAccessFile(file, "rw");
		rowSizeInBytes = 0;
		for (int i = 0; i < columnSizes.length; i++) {
			columnOffsets[i] = rowSizeInBytes;
			int colSize = columnSizes[i];
			rowSizeInBytes += colSize;
		}
		int totalSize = rowSizeInBytes * rows;
		byteBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0,
				totalSize);
	}

	public FixedByteWidthRowColDataFileWriter(ByteBuffer byteBuffer, int rows,
			int cols, int[] columnSizes) throws Exception {
		this.rows = rows;
		this.cols = cols;
		this.columnOffsets = new int[cols];
		rowSizeInBytes = 0;
		for (int i = 0; i < columnSizes.length; i++) {
			columnOffsets[i] = rowSizeInBytes;
			int colSize = columnSizes[i];
			rowSizeInBytes += colSize;
		}
		this.byteBuffer = byteBuffer;
	}

	public boolean open() {
		return true;
	}

	public void setChar(int row, int col, char ch) {
		int offset = rowSizeInBytes * row + columnOffsets[col];
		byteBuffer.putChar(offset, ch);
	}

	public void setInt(int row, int col, int i) {
		int offset = rowSizeInBytes * row + columnOffsets[col];
		byteBuffer.putInt(offset, i);
	}

	public void setShort(int row, int col, short s) {
		int offset = rowSizeInBytes * row + columnOffsets[col];
		byteBuffer.putShort(offset, s);
	}

	public void setLong(int row, int col, long l) {
		int offset = rowSizeInBytes * row + columnOffsets[col];
		byteBuffer.putLong(offset, l);
	}

	public void setFloat(int row, int col, float f) {
		int offset = rowSizeInBytes * row + columnOffsets[col];
		byteBuffer.putFloat(offset, f);
	}

	public void setDouble(int row, int col, double d) {
		int offset = rowSizeInBytes * row + columnOffsets[col];
		byteBuffer.putDouble(offset, d);
	}

	public void setString(int row, int col, String string) {
		setBytes(row, col, string.getBytes(Charset.forName("UTF-8")));
	}

	public void setBytes(int row, int col, byte[] bytes) {
		int offset = rowSizeInBytes * row + columnOffsets[col];
		byteBuffer.position(offset);
		byteBuffer.put(bytes);
	}

	public boolean saveAndClose() {
		if (raf != null) {
			try {
				raf.close();
			} catch (IOException e) {
				return false;
			}
		}
		return true;
	}
}

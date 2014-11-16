package com.linkedin.pinot.core.index.writer.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.linkedin.pinot.core.util.CustomBitSet;

public class FixedBitWidthRowColDataFileWriter {
	private File file;
	private int cols;
	private int[] columnOffsetsInBits;
	private int rows;
	private ByteBuffer byteBuffer;
	private RandomAccessFile raf;
	private int rowSizeInBits;
	private int[] columnSizesInBits;
	private CustomBitSet bitSet;

	public FixedBitWidthRowColDataFileWriter(File file, int rows, int cols,
			int[] columnSizesInBits) throws Exception {
		this.file = file;
		this.rows = rows;
		this.cols = cols;
		this.columnSizesInBits = columnSizesInBits;
		this.columnOffsetsInBits = new int[cols];
		raf = new RandomAccessFile(file, "rw");
		rowSizeInBits = 0;
		for (int i = 0; i < columnSizesInBits.length; i++) {
			columnOffsetsInBits[i] = rowSizeInBits;
			int colSize = columnSizesInBits[i];
			rowSizeInBits += colSize;
		}
		int totalSizeInBits = rowSizeInBits * rows;
		int bytesRequired = (totalSizeInBits + 7) / 8;
		byteBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0,
				bytesRequired);
		byteBuffer.position(0);
		for (int i = 0; i < bytesRequired; i++) {
			byteBuffer.put((byte) 0);
		}
		bitSet = CustomBitSet.withByteBuffer(bytesRequired, byteBuffer);
	}

	public FixedBitWidthRowColDataFileWriter(ByteBuffer byteBuffer, int rows,
			int cols, int[] columnSizesInBits) throws Exception {
		this.rows = rows;
		this.cols = cols;
		this.columnSizesInBits = columnSizesInBits;
		this.columnOffsetsInBits = new int[cols];
		rowSizeInBits = 0;
		for (int i = 0; i < columnSizesInBits.length; i++) {
			columnOffsetsInBits[i] = rowSizeInBits;
			int colSize = columnSizesInBits[i];
			rowSizeInBits += colSize;
		}
		this.byteBuffer = byteBuffer;
		int totalSizeInBits = rowSizeInBits * rows;
		int bytesRequired = (totalSizeInBits + 7) / 8;
		bitSet = CustomBitSet.withByteBuffer(bytesRequired, byteBuffer);
	}

	public boolean open() {
		return true;
	}

	/**
	 * 
	 * @param row
	 * @param col
	 * @param i
	 */
	public void setInt(int row, int col, int i) {
		assert i < Math.pow(2, columnSizesInBits[col]);
		int bitOffset = rowSizeInBits * row + columnOffsetsInBits[col];
		for (int bitPos = columnSizesInBits[col] - 1; bitPos >= 0; bitPos--) {
			if ((i & (1 << bitPos)) != 0) {
				bitSet.setBit(bitOffset + (columnSizesInBits[col] - bitPos - 1));
			}
		}

		/*
		 * byteBuffer.position(byteOffset);
		 * 
		 * int bytesToRead = (columnSizesInBits[col] + 7) / 8; byte[] dest = new
		 * byte[bytesToRead]; byteBuffer.get(dest); BitSet set =
		 * BitSet.valueOf(dest);
		 * 
		 * for (int bit = 0; bit < columnSizesInBits[col]; bit++) { if (((i >>
		 * bit) & 1) == 1) { set.set((bitOffset - (byteOffset * 8)) + bit); } }
		 * byteBuffer.position(byteOffset); byteBuffer.put(set.toByteArray());
		 */
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

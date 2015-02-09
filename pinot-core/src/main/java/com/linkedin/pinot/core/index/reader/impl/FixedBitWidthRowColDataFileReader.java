package com.linkedin.pinot.core.index.reader.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.indexsegment.utils.GenericRowColumnDataFileReader;
import com.linkedin.pinot.core.util.CustomBitSet;

/**
 *
 * Generic utility class to read data from file. The data file consists of rows
 * and columns. The number of columns are fixed. Each column can have either
 * single value or multiple values. There are two basic types of methods to read
 * the data <br/>
 * 1. <TYPE> getType(int row, int col) this is used for single value column <br/>
 * 2. int getTYPEArray(int row, int col, TYPE[] array). The caller has to create
 * and initialize the array. The implementation will fill up the array. The
 * caller is responsible to ensure that the array is big enough to fit all the
 * values. The return value tells the number of values.<br/>
 *
 * @author kgopalak
 *
 */
public class FixedBitWidthRowColDataFileReader {
	private static Logger logger = LoggerFactory
			.getLogger(GenericRowColumnDataFileReader.class);

	RandomAccessFile file;
	private int rows;
	private int cols;
	private int[] colBitOffSets;
	private int rowSizeInBits;
	private ByteBuffer byteBuffer;
	private int[] colSizesInBits;

	/**
	 * used to get the actual value val - offset. offset is non zero if the
	 * values contain negative numbers
	 */
	private int[] offsets;
	private final CustomBitSet customBitSet;

	private int totalSize;

	/**
	 *
	 * @param file
	 * @param rows
	 * @param cols
	 * @param columnSizes
	 * @return
	 * @throws IOException
	 */
	public static FixedBitWidthRowColDataFileReader forHeap(File file,
			int rows, int cols, int[] columnSizesInBits) throws IOException {
		boolean[] signed = new boolean[cols];
		Arrays.fill(signed, false);
		return new FixedBitWidthRowColDataFileReader(file, rows, cols,
				columnSizesInBits, signed, false);
	}

	/**
	 * 
	 * @param file
	 * @param rows
	 * @param cols
	 * @param columnSizesInBits
	 * @param signed
	 * @return
	 * @throws IOException
	 */
	public static FixedBitWidthRowColDataFileReader forHeap(File file,
			int rows, int cols, int[] columnSizesInBits, boolean[] signed)
			throws IOException {
		return new FixedBitWidthRowColDataFileReader(file, rows, cols,
				columnSizesInBits, signed, false);
	}

	/**
	 *
	 * @param file
	 * @param rows
	 * @param cols
	 * @param columnSizes
	 * @return
	 * @throws IOException
	 */
	public static FixedBitWidthRowColDataFileReader forMmap(File file,
			int rows, int cols, int[] columnSizesInBits) throws IOException {
		boolean[] signed = new boolean[cols];
		Arrays.fill(signed, false);
		return new FixedBitWidthRowColDataFileReader(file, rows, cols,
				columnSizesInBits, signed, true);
	}

	/**
	 * 
	 * @param file
	 * @param rows
	 * @param cols
	 * @param columnSizesInBits
	 * @param signed
	 * @return
	 * @throws IOException
	 */
	public static FixedBitWidthRowColDataFileReader forMmap(File file,
			int rows, int cols, int[] columnSizesInBits, boolean[] signed)
			throws IOException {
		return new FixedBitWidthRowColDataFileReader(file, rows, cols,
				columnSizesInBits, signed, true);
	}

	/**
	 * 
	 * @param dataBuffer
	 * @param rows
	 * @param cols
	 * @param columnSizesInBits
	 * @param signed
	 * @return
	 * @throws IOException
	 */
	public static FixedBitWidthRowColDataFileReader forByteBuffer(
			ByteBuffer dataBuffer, int rows, int cols, int[] columnSizesInBits,
			boolean[] signed) throws IOException {
		return new FixedBitWidthRowColDataFileReader(dataBuffer, rows, cols,
				columnSizesInBits, signed);
	}

	/**
	 *
	 * @param fileName
	 * @param rows
	 * @param cols
	 * @param columnSizes
	 *            in bytes
	 * @param signed
	 *            , true if the data consists of negative numbers
	 * @param isMmap
	 *            heap or mmmap
	 * @throws IOException
	 */
	private FixedBitWidthRowColDataFileReader(File dataFile, int rows,
			int cols, int[] columnSizesInBits, boolean[] signed, boolean isMmap)
			throws IOException {
		init(rows, cols, columnSizesInBits, signed);
		file = new RandomAccessFile(dataFile, "rw");
		if (isMmap) {
			byteBuffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY,
					0, totalSize);
		} else {
			byteBuffer = ByteBuffer.allocate(totalSize);
			file.getChannel().read(byteBuffer);
		}
		customBitSet = CustomBitSet.withByteBuffer(totalSize, byteBuffer);
	}

	/**
	 *
	 * @param fileName
	 * @param rows
	 * @param cols
	 * @param columnSizes
	 *            in bytes
	 * @param offsets
	 *            offset to each element to make it non negative
	 * @throws IOException
	 */
	private FixedBitWidthRowColDataFileReader(ByteBuffer buffer, int rows,
			int cols, int[] columnSizesInBits, boolean[] signed)
			throws IOException {
		this.byteBuffer = buffer;
		init(rows, cols, columnSizesInBits, signed);
		customBitSet = CustomBitSet.withByteBuffer(totalSize, byteBuffer);
	}

	/**
	 * 
	 * @param fileName
	 * @param rows
	 * @param cols
	 * @param columnSizes
	 * @throws IOException
	 */
	private FixedBitWidthRowColDataFileReader(String fileName, int rows,
			int cols, int[] columnSizes, boolean[] signed) throws IOException {
		this(new File(fileName), rows, cols, columnSizes, signed, true);
	}

	private void init(int rows, int cols, int[] columnSizesInBits,
			boolean[] signed) {

		this.rows = rows;
		this.cols = cols;
		this.colSizesInBits = new int[cols];
		rowSizeInBits = 0;
		colBitOffSets = new int[cols];
		offsets = new int[cols];
		for (int i = 0; i < columnSizesInBits.length; i++) {
			colBitOffSets[i] = rowSizeInBits;
			int colSize = columnSizesInBits[i];
			offsets[i] =0;
			if (signed[i]) {
				offsets[i] = (int) Math.pow(2, colSize) - 1;
				colSize += 1;
			}
			colSizesInBits[i] = colSize;
			rowSizeInBits += colSize;
		}
		totalSize = rowSizeInBits * rows;

	}

	/**
	 * Computes the bit offset where the actual column data can be read
	 *
	 * @param row
	 * @param col
	 * @return
	 */
	private int computeBitOffset(int row, int col) {
		if (row >= rows || col >= cols) {
			final String message = String.format(
					"Input (%d,%d) is not with in expected range (%d,%d)", row,
					col, rows, cols);
			throw new IndexOutOfBoundsException(message);
		}
		final int offset = row * rowSizeInBits + colBitOffSets[col];
		return offset;
	}

	/**
	 *
	 * @param row
	 * @param col
	 * @return
	 */
	public int getInt(int row, int col) {
		final int startBitOffset = computeBitOffset(row, col);
		final int endBitOffset = startBitOffset + colSizesInBits[col];
		int ret = customBitSet.readInt(startBitOffset, endBitOffset);
		ret = ret - offsets[col];
		return ret;

	}

	public int getNumberOfRows() {
		return rows;
	}

	public int getNumberOfCols() {
		return rows;
	}

	public int[] getColumnSizes() {
		return colSizesInBits;
	}

	public void close() {
		try {
			file.close();
		} catch (final IOException e) {
			logger.error(e.getMessage());
		}
	}

	public boolean open() {
		return true;
	}

}

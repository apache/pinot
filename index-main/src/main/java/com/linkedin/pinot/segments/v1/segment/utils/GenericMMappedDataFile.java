package com.linkedin.pinot.segments.v1.segment.utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


/**
 * Jun 30, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class GenericMMappedDataFile {
  RandomAccessFile file;
  private final int rows;
  private final int cols;
  private final int[] colOffSets;
  private int rowSize;
  private MappedByteBuffer mappedByteBuffer;
  private final int[] columnSizes;

  /**
   * 
   * @param fileName
   * @param rows
   * @param cols
   * @param columnSizes in bytes
   * @throws IOException
   */
  public GenericMMappedDataFile(File fileToMMap, int rows, int cols, int[] columnSizes) throws IOException {
    this.rows = rows;
    this.cols = cols;
    this.columnSizes = columnSizes;
    colOffSets = new int[columnSizes.length];
    for (int i = 0; i < columnSizes.length; i++) {
      if (i == 0) {
        colOffSets[i] = 0;
      } else {
        colOffSets[i] = columnSizes[i - 1];
      }
      rowSize += columnSizes[i];
    }
    file = new RandomAccessFile(fileToMMap, "rw");
    long totalSize = rowSize * rows;
    mappedByteBuffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, totalSize);
  }

  public GenericMMappedDataFile(String fileName, int rows, int cols, int[] columnSizes) throws IOException {
    this(new File(fileName), rows, cols, columnSizes);
  }

  /**
   * Computes the offset where the actual column data can be read
   * @param row
   * @param col
   * @return
   */
  private int computeOffset(int row, int col) {
    if (row >= rows || col >= cols) {
      String message = String.format("Input (%d,%d) is not with in expected range (%s,%s)", row, col, rows, cols);
      throw new IndexOutOfBoundsException(message);
    }
    int offset = row * rowSize + colOffSets[col];
    return offset;
  }

  /**
   * 
   * @param row
   * @param col
   * @return
   */
  public char getChar(int row, int col) {
    int offset = computeOffset(row, col);
    return mappedByteBuffer.getChar(offset);
  }

  /**
   * 
   * @param row
   * @param col
   * @return
   */
  public short getShort(int row, int col) {
    int offset = computeOffset(row, col);
    return mappedByteBuffer.getShort(offset);
  }

  /**
   * 
   * @param row
   * @param col
   * @return
   */
  public int getInt(int row, int col) {
    assert getColumnSizes()[col] == 4;
    int offset = computeOffset(row, col);
    return mappedByteBuffer.getInt(offset);
  }

  /**
   * 
   * @param row
   * @param col
   * @return
   */
  public long getLong(int row, int col) {
    assert getColumnSizes()[col] == 8;
    int offset = computeOffset(row, col);
    return mappedByteBuffer.getLong(offset);
  }

  /**
   * 
   * @param row
   * @param col
   * @return
   */
  public float getFloat(int row, int col) {
    assert getColumnSizes()[col] == 8;
    int offset = computeOffset(row, col);
    return mappedByteBuffer.getFloat(offset);
  }

  /**
   * Reads the double at row,col
   * @param row
   * @param col
   * @return
   */
  public double getDouble(int row, int col) {
    assert getColumnSizes()[col] == 8;
    int offset = computeOffset(row, col);
    return mappedByteBuffer.getDouble(offset);
  }

  /**
   * Returns the string value, NOTE: It expects all String values in the file to be of same length
   * @param row
   * @param col
   * @return
   */
  public String getString(int row, int col) {
    return new String(get(row, col));
  }

  /**
   * Generic method to read the raw bytes
   * @param row
   * @param col
   * @return
   */
  public byte[] get(int row, int col) {
    int length = getColumnSizes()[col];
    byte[] dst = new byte[length];
    int offset = computeOffset(row, col);
    mappedByteBuffer.position(offset);
    mappedByteBuffer.get(dst, 0, length);
    return dst;
  }

  public int getNumberOfRows() {
    return rows;
  }

  public int getNumberOfCols() {
    return rows;
  }

  public int[] getColumnSizes() {
    return columnSizes;
  }
}

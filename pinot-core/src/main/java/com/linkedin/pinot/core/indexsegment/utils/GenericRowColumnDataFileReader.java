package com.linkedin.pinot.core.indexsegment.utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Jun 30, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class GenericRowColumnDataFileReader {
  private static Logger logger = LoggerFactory.getLogger(GenericRowColumnDataFileReader.class);

  RandomAccessFile file;
  private final int rows;
  private final int cols;
  private final int[] colOffSets;
  private int rowSize;
  private ByteBuffer byteBuffer;
  private final int[] columnSizes;

  /**
   * 
   * @param file
   * @param rows
   * @param cols
   * @param columnSizes
   * @return
   * @throws IOException 
   */
  public static GenericRowColumnDataFileReader forHeap(File file, int rows, int cols, int[] columnSizes)
      throws IOException {
    return new GenericRowColumnDataFileReader(file, rows, cols, columnSizes, false);
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
  public static GenericRowColumnDataFileReader forMmap(File file, int rows, int cols, int[] columnSizes)
      throws IOException {
    return new GenericRowColumnDataFileReader(file, rows, cols, columnSizes, true);
  }

  /**
   * 
   * @param fileName
   * @param rows
   * @param cols
   * @param columnSizes in bytes
   * @throws IOException
   */
  public GenericRowColumnDataFileReader(File dataFile, int rows, int cols, int[] columnSizes, boolean isMmap)
      throws IOException {
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
    file = new RandomAccessFile(dataFile, "rw");
    long totalSize = rowSize * rows;
    if (isMmap)
      byteBuffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, totalSize);
    else {
      byteBuffer = ByteBuffer.allocate((int) totalSize);
      file.getChannel().read(byteBuffer);
    }  
  }

  public GenericRowColumnDataFileReader(String fileName, int rows, int cols, int[] columnSizes) throws IOException {
    this(new File(fileName), rows, cols, columnSizes, true);
  }

  /**
   * Computes the offset where the actual column data can be read
   * @param row
   * @param col
   * @return
   */
  private int computeOffset(int row, int col) {
    if (row >= rows || col >= cols) {
      String message = String.format("Input (%d,%d) is not with in expected range (%d,%d)", row, col, rows, cols);
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
    return byteBuffer.getChar(offset);
  }

  /**
   * 
   * @param row
   * @param col
   * @return
   */
  public short getShort(int row, int col) {
    int offset = computeOffset(row, col);
    return byteBuffer.getShort(offset);
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
    return byteBuffer.getInt(offset);
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
    return byteBuffer.getLong(offset);
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
    return byteBuffer.getFloat(offset);
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
    return byteBuffer.getDouble(offset);
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
    byteBuffer.position(offset);
    byteBuffer.get(dst, 0, length);
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

  public void close() {
    try {
      file.close();
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
  }
}

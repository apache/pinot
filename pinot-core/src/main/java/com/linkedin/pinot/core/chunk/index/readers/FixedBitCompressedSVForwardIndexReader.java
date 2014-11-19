package com.linkedin.pinot.core.chunk.index.readers;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.core.index.reader.DataFileMetadata;
import com.linkedin.pinot.core.index.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.index.reader.impl.FixedBitWidthRowColDataFileReader;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 13, 2014
 */

public class FixedBitCompressedSVForwardIndexReader implements SingleColumnSingleValueReader {

  private final File indexFile;
  private final FixedBitWidthRowColDataFileReader dataFileReader;
  private final int rows;

  public FixedBitCompressedSVForwardIndexReader(File file, int rows, int columnSize, boolean isMMap) throws IOException {
    indexFile = file;
    if (isMMap) {
      dataFileReader = FixedBitWidthRowColDataFileReader.forMmap(indexFile, rows, 1, new int[] { columnSize });
    } else {
      dataFileReader = FixedBitWidthRowColDataFileReader.forHeap(indexFile, rows, 1, new int[] { columnSize });
    }

    this.rows = rows;
  }

  public int getLength() {
    return rows;
  }

  @Override
  public boolean open() {
    dataFileReader.open();
    return true;
  }

  @Override
  public DataFileMetadata getMetadata() {
    return null;
  }

  @Override
  public boolean close() {
    dataFileReader.close();
    return true;
  }

  @Override
  public char getChar(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShort(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int row) {
    return dataFileReader.getInt(row, 0);
  }

  @Override
  public long getLong(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytes(int row) {
    throw new UnsupportedOperationException();
  }

}

/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.readers;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.core.index.reader.DataFileMetadata;
import com.linkedin.pinot.core.index.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.index.reader.impl.FixedBitWidthRowColDataFileReader;


/**
 * Nov 13, 2014
 */

public class FixedBitCompressedSVForwardIndexReader implements SingleColumnSingleValueReader {

  private final File indexFile;
  private final FixedBitWidthRowColDataFileReader dataFileReader;
  private final int rows;

  public FixedBitCompressedSVForwardIndexReader(File file, int rows, int columnSize, boolean isMMap, boolean hasNulls) throws IOException {
    indexFile = file;
    if (isMMap) {
      dataFileReader = FixedBitWidthRowColDataFileReader.forMmap(indexFile, rows, 1, new int[] { columnSize }, new boolean[] { hasNulls });
    } else {
      dataFileReader = FixedBitWidthRowColDataFileReader.forHeap(indexFile, rows, 1, new int[] { columnSize }, new boolean[] { hasNulls });
    }

    this.rows = rows;
  }

  public int getLength() {
    return rows;
  }

  @Override
  public DataFileMetadata getMetadata() {
    return null;
  }

  @Override
  public void close() throws IOException {
    dataFileReader.close();
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

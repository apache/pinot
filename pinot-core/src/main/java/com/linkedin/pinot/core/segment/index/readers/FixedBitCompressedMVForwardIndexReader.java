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

import com.linkedin.pinot.common.utils.MmapUtils;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.index.reader.DataFileMetadata;
import com.linkedin.pinot.core.index.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.index.reader.impl.FixedBitWidthRowColDataFileReader;
import com.linkedin.pinot.core.index.reader.impl.FixedByteWidthRowColDataFileReader;

/**
 * Reads a column where each row can have multiple values. Lets say we want to
 * represent (1,2,4) <br>
 * (3,4) <br>
 * (5,6)<br>
 * Overall there are (3+2+2) = 7 values that we need to store. We will store
 * them sequentially but by storing sequentially we dont know the boundary
 * between each row. We store additional header section that tells the offset
 * and length for each row.
 *
 * The file can be represented as two sections <br>
 *
 * <pre>
 * {@code
 * Header <br>
 * (rowOffset, number of values)
 * 0 3
 * 3 2
 * 5 2
 * Data
 * 1
 * 2
 * 4
 * 3
 * 4
 * 5
 * 6
 * }
 * </pre>
 *
 * so if want to read the values for docId=0. We first read the header values
 * (offset and length) at offset 0, in this case we get 6,3. Now we read, 6
 * integer
 *
 *
 */
public class FixedBitCompressedMVForwardIndexReader implements
    SingleColumnMultiValueReader {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(FixedBitCompressedMVForwardIndexReader.class);
  private static int SIZE_OF_INT = 4;
  private static int NUM_COLS_IN_HEADER = 2;
  private RandomAccessFile raf;
  private FixedByteWidthRowColDataFileReader headerSectionReader;
  private FixedBitWidthRowColDataFileReader dataSectionReader;
  private int rows;
  private int totalNumValues;
  private ByteBuffer dataBuffer;

  /**
   * @param file
   * @param numDocs
   * @param columnSizeInBits
   * @param isMMap
   */
  public FixedBitCompressedMVForwardIndexReader(File file, int numDocs,
      int columnSizeInBits, boolean isMMap) throws Exception {
    boolean[] signed = new boolean[1];
    Arrays.fill(signed, false);
    init(file, numDocs, columnSizeInBits, signed, isMMap);
  }

  /**
   * @param file
   * @param numDocs
   * @param columnSizeInBits
   * @param isMMap
   */
  public FixedBitCompressedMVForwardIndexReader(File file, int numDocs,
      int columnSizeInBits, boolean[] signed, boolean isMMap)
      throws Exception {
    init(file, numDocs, columnSizeInBits, signed, isMMap);

  }

  private void init(File file, int numDocs, int columnSizeInBits,
      boolean[] signed, boolean isMMap) throws FileNotFoundException,
      IOException {
    // compute the header size= numDocs * size of int * 2
    rows = numDocs;
    final int headerSize = numDocs * SIZE_OF_INT * NUM_COLS_IN_HEADER;
    ByteBuffer byteBuffer;
    raf = new RandomAccessFile(file, "rw");
    if (isMMap) {
      byteBuffer = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0,
          headerSize);

    } else {
      byteBuffer = ByteBuffer.allocateDirect(headerSize);
      raf.getChannel().read(byteBuffer);

    }
    headerSectionReader = new FixedByteWidthRowColDataFileReader(
        byteBuffer, numDocs, NUM_COLS_IN_HEADER, new int[] {
            SIZE_OF_INT, SIZE_OF_INT });

    // to calculate total number of values across all docs, we need to read
    // the last two entries in the header (start index, length)
    final int startIndex = headerSectionReader.getInt(numDocs - 1, 0);
    final int length = headerSectionReader.getInt(numDocs - 1, 1);
    totalNumValues = startIndex + length;
    final int dataSizeInBytes = ((totalNumValues * columnSizeInBits) + 7) / 8;

    if (isMMap) {
      dataBuffer = raf.getChannel().map(FileChannel.MapMode.READ_ONLY,
          headerSize, dataSizeInBytes);
    } else {
      dataBuffer = ByteBuffer.allocateDirect(dataSizeInBytes);
      raf.getChannel().read(dataBuffer, headerSize);
    }
    dataSectionReader =  FixedBitWidthRowColDataFileReader.forByteBuffer(dataBuffer,
        totalNumValues, 1, new int[] { columnSizeInBits }, signed);
  }

  public int getTotalNumValues() {
    return totalNumValues;
  }

  public int length() {
    return rows;
  }


  @Override
  public DataFileMetadata getMetadata() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() throws IOException{
    IOUtils.closeQuietly(raf);
    MmapUtils.unloadByteBuffer(dataBuffer);
  }

  @Override
  public int getCharArray(int row, char[] charArray) {
    throw new UnsupportedOperationException(
        "Only int data types are allowed in multivalue fixed bit format");
  }

  @Override
  public int getShortArray(int row, short[] shortArray) {
    throw new UnsupportedOperationException(
        "Only int data types are allowed in multivalue fixed bit format");

  }

  @Override
  public int getIntArray(int row, int[] intArray) {
    final int startIndex = headerSectionReader.getInt(row, 0);
    final int length = headerSectionReader.getInt(row, 1);
    for (int i = 0; i < length; i++) {
      intArray[i] = dataSectionReader.getInt(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int getLongArray(int row, long[] longArray) {
    throw new UnsupportedOperationException(
        "Only int data types are allowed in multivalue fixed bit format");

  }

  @Override
  public int getFloatArray(int row, float[] floatArray) {
    throw new UnsupportedOperationException(
        "Only int data types are allowed in multivalue fixed bit format");

  }

  @Override
  public int getDoubleArray(int row, double[] doubleArray) {
    throw new UnsupportedOperationException(
        "Only int data types are allowed in multivalue fixed bit format");

  }

  @Override
  public int getStringArray(int row, String[] stringArray) {
    throw new UnsupportedOperationException(
        "Only int data types are allowed in multivalue fixed bit format");

  }

  @Override
  public int getBytesArray(int row, byte[][] bytesArray) {
    throw new UnsupportedOperationException(
        "Only int data types are allowed in multivalue fixed bit format");

  }

}

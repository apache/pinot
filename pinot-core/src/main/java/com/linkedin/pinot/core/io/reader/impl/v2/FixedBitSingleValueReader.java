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
package com.linkedin.pinot.core.io.reader.impl.v2;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.utils.MmapUtils;
import com.linkedin.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.util.SizeUtil;

import me.lemire.integercompression.BitPacking;

/**
 * Reads integers that were bit compressed. It unpacks 32 values at a time (since these are aligned
 * to integer boundary). Which means it reads 32 * numBits/8 bytes at a time.
 * for e.g if its 3 bit compressed, then we read 3 int (96 bytes at one go).
 * This may seem over kill because in order to read 1 value, we uncompress 32 values.
 * But in reality the cost get amortized when we read a range of values.
 */
public class FixedBitSingleValueReader extends BaseSingleColumnSingleValueReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedBitSingleValueReader.class);
  private int compressedSize;
  private int uncompressedSize;
  private RandomAccessFile file;
  private int rows;
  private ByteBuffer byteBuffer;
  private boolean ownsByteBuffer;
  private boolean isMmap;
  private BitUnpackResultWrapper bitUnpackWrapper;
  private int numBits;

  /**
   * @param file
   * @param rows
   * @param columnSizesInBits
   * @return
   * @throws IOException
   */
  public static FixedBitSingleValueReader forHeap(File file, int rows, int numBits)
      throws IOException {
    boolean signed = false;
    return new FixedBitSingleValueReader(file, rows, numBits, signed, false);
  }

  /**
   * @param file
   * @param rows
   * @param numBits
   * @param signed
   * @return
   * @throws IOException
   */
  public static FixedBitSingleValueReader forHeap(File file, int rows, int numBits, boolean signed)
      throws IOException {
    return new FixedBitSingleValueReader(file, rows, numBits, signed, false);
  }

  /**
   * @param file
   * @param rows
   * @param numBits
   * @return
   * @throws IOException
   */
  public static FixedBitSingleValueReader forMmap(File file, int rows, int numBits)
      throws IOException {
    boolean signed = false;
    return new FixedBitSingleValueReader(file, rows, numBits, signed, true);
  }

  /**
   * @param file
   * @param rows
   * @param numBits
   * @param signed
   * @return
   * @throws IOException
   */
  public static FixedBitSingleValueReader forMmap(File file, int rows, int numBits, boolean signed)
      throws IOException {
    return new FixedBitSingleValueReader(file, rows, numBits, signed, true);
  }

  /**
   * @param dataBuffer
   * @param rows
   * @param numBits
   * @param signed
   * @return
   * @throws IOException
   */
  public static FixedBitSingleValueReader forByteBuffer(ByteBuffer dataBuffer, int rows,
      int numBits, boolean signed) throws IOException {
    return new FixedBitSingleValueReader(dataBuffer, rows, numBits, signed);
  }

  /**
   * @param dataFile
   * @param rows
   * @param numBits
   * @param signed
   * @param isMmap
   * @throws IOException
   */
  public FixedBitSingleValueReader(File dataFile, int rows, int numBits, boolean signed,
      boolean isMmap) throws IOException {
    init(rows, numBits, signed);
    file = new RandomAccessFile(dataFile, "rw");
    this.isMmap = isMmap;
    if (isMmap) {
      byteBuffer = MmapUtils.mmapFile(file, FileChannel.MapMode.READ_ONLY, 0, file.length(),
          dataFile, this.getClass().getSimpleName() + " byteBuffer");
    } else {
      byteBuffer = MmapUtils.allocateDirectByteBuffer((int) file.length(), dataFile,
          this.getClass().getSimpleName() + " byteBuffer");
      file.getChannel().read(byteBuffer);
      file.close();
    }
    LOGGER.info("Loaded file:{} of size:{}", dataFile.getName(), dataFile.length());
    // unpack 32 values at a time.
    ownsByteBuffer = true;
  }

  /**
   * @param buffer
   * @param rows
   * @param numBits
   * @param signed
   * @throws IOException
   */
  private FixedBitSingleValueReader(ByteBuffer buffer, int rows, int numBits, boolean signed)
      throws IOException {
    this.byteBuffer = buffer;
    ownsByteBuffer = false;
    this.isMmap = false;
    init(rows, numBits, signed);
  }

  /**
   * @param fileName
   * @param rows
   * @param columnSizes
   * @throws IOException
   */
  private FixedBitSingleValueReader(String fileName, int rows, int numBits, boolean signed)
      throws IOException {
    this(new File(fileName), rows, numBits, signed, true);
  }

  private void init(int rows, int numBits, boolean signed) {

    this.rows = rows;
    this.numBits = numBits;
    // we always read 32 values at a time
    this.uncompressedSize = SizeUtil.BIT_UNPACK_BATCH_SIZE;
    // how many ints are required to store 32 values <br/>
    // uncompressedSize * numBits/ SIZE_OF_INT = numBits [since uncompressedSize == SIZE_OF_INT ==32
    // ]
    this.compressedSize = numBits;
    if (signed) {
      numBits = numBits + 1;
      // totalSizeInBytes = (int) (((((long) (numBits + 1)) * rows) + 7) / 8);
    } else {
      // totalSizeInBytes = (int) (((((long) numBits) * rows) + 7) / 8);
    }
    bitUnpackWrapper = new BitUnpackResultWrapper(compressedSize, uncompressedSize);
  }

  /**
   * @param rowIds assumes rowIds are sorted
   * @param values
   * @param length
   */
  public void getIntBatch(int startRow, int length, int[] values) {
    int counter = 0;
    BitUnpackResult tempResult = bitUnpackWrapper.get();
    while (counter < length) {
      int batchPosition = (startRow + counter) / uncompressedSize;
      if (tempResult.position != batchPosition) {
        int startIndex = batchPosition * numBits * 4;
        for (int i = 0; i < numBits; i++) {
          tempResult.compressed[i] = byteBuffer.getInt(startIndex + i * 4);
        }
        BitPacking.fastunpack(tempResult.compressed, 0, tempResult.uncompressed, 0, numBits);
      }
      int endRowId = (batchPosition + 1) * uncompressedSize;
      while (counter < length && (startRow + counter) < endRowId) {
        values[counter] = tempResult.uncompressed[(startRow + counter) % uncompressedSize];
        counter = counter + 1;
      }
    }
  }

  /**
   * @param rowIds assumes rowIds are sorted
   * @param values
   * @param length
   */
  public void getIntBatch(int rowIds[], int[] values, int length) {
    int counter = 0;
    BitUnpackResult tempResult = bitUnpackWrapper.get();
    while (counter < length) {
      int batchPosition = rowIds[counter] / uncompressedSize;
      if (tempResult.position != batchPosition) {
        int startIndex = batchPosition * numBits * 4;
        for (int i = 0; i < numBits; i++) {
          tempResult.compressed[i] = byteBuffer.getInt(startIndex + i * 4);
        }
        BitPacking.fastunpack(tempResult.compressed, 0, tempResult.uncompressed, 0, numBits);
      }
      int endRowId = (batchPosition + 1) * uncompressedSize;
      while (counter < length && rowIds[counter] < endRowId) {
        values[counter] = tempResult.uncompressed[rowIds[counter] % uncompressedSize];
        counter = counter + 1;
      }
    }
  }

  /**
   * @param row
   * @return
   */
  public int getInt(int row) {
    BitUnpackResult result = bitUnpackWrapper.get();
    int batchPosition = row / uncompressedSize;
    if (result.position != batchPosition) {
      int index = -1;
      int startIndex = batchPosition * numBits * 4;
      for (int i = 0; i < numBits; i++) {
        try {
          index = startIndex + i * 4;
          result.compressed[i] = byteBuffer.getInt(index);
        } catch (Exception e) {
          LOGGER.error("Exception while retreiving value for row:{} at index:{} numBits:{}", row,
              index, numBits, e);
          throw e;
        }
      }
      BitPacking.fastunpack(result.compressed, 0, result.uncompressed, 0, numBits);
      result.position = batchPosition;
      // bitUnpackWrapper.set(result);
    }
    int ret = result.uncompressed[row % uncompressedSize];
    return ret;

  }

  public int getNumberOfRows() {
    return rows;
  }

  @Override
  public void close() throws IOException {
    if (ownsByteBuffer) {
      MmapUtils.unloadByteBuffer(byteBuffer);
      byteBuffer = null;

      if (isMmap) {
        file.close();
      }
    }
  }

  public boolean open() {
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

  @Override
  public void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos) {
    int endPos = rowStartPos + rowSize;
    for (int ri = rowStartPos; ri < endPos; ++ri) {
      values[valuesStartPos++] = getInt(rows[ri]);
    }
  }
}

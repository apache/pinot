/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.util.SizeUtil;
import java.io.IOException;
import me.lemire.integercompression.BitPacking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads integers that were bit compressed. It unpacks 32 values at a time (since these are aligned
 * to integer boundary). Which means it reads 32 * numBits/8 bytes at a time.
 * for e.g if its 3 bit compressed, then we read 3 int (96 bytes at one go).
 * This may seem over kill because in order to read 1 value, we decompress 32 values.
 * But in reality the cost get amortized when we read a range of values.
 */
public class FixedBitSingleValueReader extends BaseSingleColumnSingleValueReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedBitSingleValueReader.class);
  private int compressedSize;
  private int uncompressedSize;
  private int rows;
  private PinotDataBuffer indexDataBuffer;
  private BitUnpackResultWrapper bitUnpackWrapper;
  private int numBits;

  /**
   * @param dataBuffer
   * @param rows
   * @param numBits
   * @param signed
   * @throws IOException
   */
  public FixedBitSingleValueReader(PinotDataBuffer dataBuffer, int rows, int numBits, boolean signed) {
    this.indexDataBuffer = dataBuffer;
    init(rows, numBits, signed);
  }

  public FixedBitSingleValueReader(PinotDataBuffer dataBuffer, int rows, int numBits) {
    this(dataBuffer, rows, numBits, false /*signed*/);
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
   * @param startRow assumes rowIds are sorted
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
          tempResult.compressed[i] = indexDataBuffer.getInt(startIndex + i * 4);
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
          tempResult.compressed[i] = indexDataBuffer.getInt(startIndex + i * 4);
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
          result.compressed[i] = indexDataBuffer.getInt(index);
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
    indexDataBuffer.close();
  }

  public boolean open() {
    return true;
  }

  @Override
  public void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos) {
    int endPos = rowStartPos + rowSize;
    for (int ri = rowStartPos; ri < endPos; ++ri) {
      values[valuesStartPos++] = getInt(rows[ri]);
    }
  }

  @Override
  public ReaderContext createContext() {
    //no need of context for fixedbit 
    return null;
  }
}

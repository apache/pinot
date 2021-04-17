/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.io.util;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;


public final class FixedBitIntReaderWriterV2 implements Closeable {
  private PinotDataBitSetV2 _dataBitSet;

  public FixedBitIntReaderWriterV2(PinotDataBuffer dataBuffer, int numValues, int numBitsPerValue) {
    Preconditions
        .checkState(dataBuffer.size() == (int) (((long) numValues * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE));
    _dataBitSet = PinotDataBitSetV2.createBitSet(dataBuffer, numBitsPerValue);
  }

  /**
   * Read dictionaryId for a particular docId
   * @param index docId to get the dictionaryId for
   * @return dictionaryId
   */
  public int readInt(int index) {
    return _dataBitSet.readInt(index);
  }

  /**
   * Array based API to read dictionaryIds for a contiguous
   * range of docIds starting at startDocId for a given length
   * @param startDocId docId range start
   * @param length length of contiguous docId range
   * @param buffer out buffer to read dictionaryIds into
   */
  public void readInt(int startDocId, int length, int[] buffer) {
    _dataBitSet.readInt(startDocId, length, buffer);
  }

  /**
   * Array based API to read dictionaryIds for an array of docIds
   * which are monotonically increasing but not necessarily contiguous.
   * The difference between this and previous array based API {@link #readInt(int, int, int[])}
   * is that unlike the other API, we are provided an array of docIds.
   * So even though the docIds in docIds[] array are monotonically increasing,
   * they may not necessarily be contiguous. They can have gaps.
   *
   * {@link PinotDataBitSetV2} implements efficient bulk contiguous API
   * {@link PinotDataBitSetV2#readInt(long, int, int[])}
   * to read dictionaryIds for a contiguous range of docIds represented
   * by startDocId and length.
   *
   * This API although works on docIds with gaps, it still tries to
   * leverage the underlying bulk contiguous API as much as possible to
   * get benefits of vectorization.
   *
   * For a given docIds[] array, we determine if we should use the
   * bulk contiguous API or not by checking if the length of the array
   * is >= 50% of actual docIdRange (lastDocId - firstDocId + 1). This
   * sort of gives a very rough idea of the gaps in docIds. We will benefit
   * from bulk contiguous read if the gaps are narrow implying fewer dictIds
   * unpacked as part of contiguous read will have to be thrown away/ignored.
   * If the gaps are wide, a higher number of dictIds will be thrown away
   * before we construct the out array
   *
   * This method of determining if bulk contiguous should be used or not
   * is inaccurate since it is solely dependent on the first and
   * last docId. However, getting an exact idea of the gaps in docIds[]
   * array will first require a single pass through the array to compute
   * the deviations between each docId and then take mean/stddev of that.
   * This will be expensive as it requires pre-processing.
   *
   * To increase the probability of using the bulk contiguous API, we make
   * this decision for every fixed-size chunk of docIds[] array.
   *
   * @param docIds array of docIds to read the dictionaryIds for
   * @param docIdStartIndex start index in docIds array
   * @param docIdLength length to process in docIds array
   * @param values out array to store the dictionaryIds into
   * @param valuesStartIndex start index in values array
   */
  public void readValues(int[] docIds, int docIdStartIndex, int docIdLength, int[] values, int valuesStartIndex) {
    int bulkReadChunks = docIdLength / PinotDataBitSetV2.MAX_VALUES_UNPACKED_SINGLE_ALIGNED_READ;
    int remainingChunk = docIdLength % PinotDataBitSetV2.MAX_VALUES_UNPACKED_SINGLE_ALIGNED_READ;
    int docIdEndIndex;
    while (bulkReadChunks > 0) {
      docIdEndIndex = docIdStartIndex + PinotDataBitSetV2.MAX_VALUES_UNPACKED_SINGLE_ALIGNED_READ - 1;
      if (shouldBulkRead(docIds, docIdStartIndex, docIdEndIndex)) {
        // use the bulk API. it takes care of populating the values array correctly
        // by throwing away the extra dictIds
        _dataBitSet.readInt(docIds, docIdStartIndex, PinotDataBitSetV2.MAX_VALUES_UNPACKED_SINGLE_ALIGNED_READ, values,
            valuesStartIndex);
        valuesStartIndex += PinotDataBitSetV2.MAX_VALUES_UNPACKED_SINGLE_ALIGNED_READ;
      } else {
        // use the single read API
        for (int i = docIdStartIndex; i <= docIdEndIndex; i++) {
          values[valuesStartIndex++] = _dataBitSet.readInt(docIds[i]);
        }
      }
      docIdStartIndex += PinotDataBitSetV2.MAX_VALUES_UNPACKED_SINGLE_ALIGNED_READ;
      bulkReadChunks--;
      docIdLength -= PinotDataBitSetV2.MAX_VALUES_UNPACKED_SINGLE_ALIGNED_READ;
    }
    if (remainingChunk > 0) {
      // use the single read API
      docIdEndIndex = docIdStartIndex + docIdLength - 1;
      for (int i = docIdStartIndex; i <= docIdEndIndex; i++) {
        values[valuesStartIndex++] = _dataBitSet.readInt(docIds[i]);
      }
    }
  }

  private boolean shouldBulkRead(int[] docIds, int startIndex, int endIndex) {
    int numDocsToRead = endIndex - startIndex + 1;
    int docIdRange = docIds[endIndex] - docIds[startIndex] + 1;
    return numDocsToRead >= ((double) docIdRange * 0.5);
  }

  public void writeInt(int index, int value) {
    _dataBitSet.writeInt(index, value);
  }

  public void writeInt(int startIndex, int length, int[] values) {
    _dataBitSet.writeInt(startIndex, length, values);
  }

  @Override
  public void close() throws IOException {
    if (_dataBitSet != null) {
      _dataBitSet.close();
    }
  }
}

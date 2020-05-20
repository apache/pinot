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
package org.apache.pinot.core.io.util;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


public final class FixedBitIntReaderWriterV2 implements Closeable {
  private volatile PinotDataBitSetV2 _dataBitSet;

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
   * which are monotonically increasing but not necessarily contiguous
   * @param docIds array of docIds to read the dictionaryIds for
   * @param docIdStartIndex start index in docIds array
   * @param docIdLength length to process in docIds array
   * @param values out array to store the dictionaryIds into
   * @param valuesStartIndex start index in values array
   */
  public void readValues(int[] docIds, int docIdStartIndex, int docIdLength, int[] values, int valuesStartIndex) {
    int docIdEndIndex = docIdStartIndex + docIdLength - 1;
    if (shouldBulkRead(docIds, docIdStartIndex, docIdEndIndex)) {
      _dataBitSet.readInt(docIds, docIdStartIndex, docIdLength, values, valuesStartIndex);
    } else {
      for (int i = docIdStartIndex; i <= docIdEndIndex; i++) {
        values[valuesStartIndex++] = _dataBitSet.readInt(docIds[i]);
      }
    }
  }

  private boolean shouldBulkRead(int[] docIds, int startIndex, int endIndex) {
    int numDocsToRead = endIndex - startIndex + 1;
    int docIdRange = docIds[endIndex] - docIds[startIndex] + 1;
    if (docIdRange > DocIdSetPlanNode.MAX_DOC_PER_CALL) {
      return false;
    }
    return numDocsToRead >= ((double)docIdRange * 0.7);
  }

  public void writeInt(int index, int value) {
    _dataBitSet.writeInt(index, value);
  }

  public void writeInt(int startIndex, int length, int[] values) {
    _dataBitSet.writeInt(startIndex, length, values);
  }

  @Override
  public void close()
      throws IOException {
    if (_dataBitSet != null) {
      _dataBitSet.close();
      _dataBitSet = null;
    }
  }
}
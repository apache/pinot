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
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


public final class FixedBitIntReaderWriter implements Closeable {
  private final PinotDataBitSet _dataBitSet;

  public FixedBitIntReaderWriter(PinotDataBuffer dataBuffer, int numValues, int numBitsPerValue) {
    Preconditions
        .checkState(dataBuffer.size() == (int) (((long) numValues * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE));
    _dataBitSet = new PinotDataBitSet(dataBuffer, numBitsPerValue);
  }

  public int readInt(int index) {
    return _dataBitSet.readInt(index);
  }

  public void readInt(int startIndex, int length, int[] buffer) {
    _dataBitSet.readInt(startIndex, length, buffer);
  }

  public void readValues(int[] docIds, int docIdStartIndex, int docIdLength, int[] values, int valuesStartIndex) {
    int docIdEndIndex = docIdStartIndex + docIdLength;
    for (int i = docIdStartIndex; i < docIdEndIndex; i++) {
      values[valuesStartIndex++] = readInt(docIds[i]);
    }
  }

  public void writeInt(int index, int value) {
    _dataBitSet.writeInt(index, value);
  }

  public void writeInt(int startIndex, int length, int[] values) {
    _dataBitSet.writeInt(startIndex, length, values);
  }

  @Override
  public void close() {
    _dataBitSet.close();
  }
}

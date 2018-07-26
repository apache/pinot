/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.io.util;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.Closeable;
import java.io.IOException;


public final class FixedBitIntReaderWriter implements Closeable {
  private final PinotDataBitSet _dataBitSet;
  private final int _numBitsPerValue;

  public FixedBitIntReaderWriter(PinotDataBuffer dataBuffer, int numValues, int numBitsPerValue) {
    Preconditions.checkState(
        dataBuffer.size() == (int) (((long) numValues * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE));
    _dataBitSet = new PinotDataBitSet(dataBuffer);
    _numBitsPerValue = numBitsPerValue;
  }

  public int readInt(int index) {
    return _dataBitSet.readInt(index, _numBitsPerValue);
  }

  public void readInt(int startIndex, int length, int[] buffer) {
    _dataBitSet.readInt(startIndex, _numBitsPerValue, length, buffer);
  }

  public void writeInt(int index, int value) {
    _dataBitSet.writeInt(index, _numBitsPerValue, value);
  }

  public void writeInt(int startIndex, int length, int[] values) {
    _dataBitSet.writeInt(startIndex, _numBitsPerValue, length, values);
  }

  @Override
  public void close() throws IOException {
    _dataBitSet.close();
  }
}

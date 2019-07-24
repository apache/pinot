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
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


public final class FixedBitIntReaderWriter implements Closeable {
  // To deal with a multi-threading scenario in query processing threads
  // (which are currently non-interruptible), a segment could be dropped by
  // the parent thread and the child query thread could still be using
  // segment memory which may have been unmapped depending on when the
  // drop was completed. To protect against this scenario, the data buffer
  // is made volatile and set to null in close() operation after releasing
  // the buffer. This ensures that concurrent thread(s) trying to invoke
  // set**() operations on this class will hit NPE as opposed accessing
  // illegal/invalid memory (which will crash the JVM).
  private volatile PinotDataBitSet _dataBitSet;
  private final int _numBitsPerValue;

  public FixedBitIntReaderWriter(PinotDataBuffer dataBuffer, int numValues, int numBitsPerValue) {
    Preconditions
        .checkState(dataBuffer.size() == (int) (((long) numValues * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE));
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
  public void close()
      throws IOException {
    if (_dataBitSet != null) {
      _dataBitSet.close();
      _dataBitSet.close();
    }
  }
}

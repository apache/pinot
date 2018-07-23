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
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.nio.ByteBuffer;


public class DimensionBuffer {
  private final ByteBuffer _buffer;

  public static DimensionBuffer fromBytes(byte[] bytes) {
    return new DimensionBuffer(bytes);
  }

  public DimensionBuffer(int numDimensions) {
    _buffer = ByteBuffer.wrap(new byte[numDimensions * Integer.BYTES]).order(PinotDataBuffer.NATIVE_ORDER);
  }

  private DimensionBuffer(byte[] bytes) {
    _buffer = ByteBuffer.wrap(bytes).order(PinotDataBuffer.NATIVE_ORDER);
  }

  public int getDictId(int index) {
    return _buffer.getInt(index * Integer.BYTES);
  }

  public void setDictId(int index, int dictId) {
    _buffer.putInt(index * Integer.BYTES, dictId);
  }

  public byte[] toBytes() {
    return _buffer.array();
  }

  // NOTE: we don't check whether the array is null or the length of the array for performance concern. All the
  // dimension buffers should have the same length.
  public boolean hasSameBytes(byte[] dimensionBytes) {
    byte[] bytes = _buffer.array();
    int numBytes = bytes.length;
    for (int i = 0; i < numBytes; i++) {
      if (bytes[i] != dimensionBytes[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("[");
    String delimiter = "";
    int numBytes = _buffer.limit();
    for (int i = 0; i < numBytes; i += Integer.BYTES) {
      builder.append(delimiter).append(_buffer.getInt(i));
      delimiter = ", ";
    }
    builder.append("]");
    return builder.toString();
  }
}

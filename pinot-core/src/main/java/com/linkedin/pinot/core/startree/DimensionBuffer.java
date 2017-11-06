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
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;


public class DimensionBuffer {
  private static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();

  private final int _numDimensions;
  private final IntBuffer _intBuffer;

  public DimensionBuffer(int numDimensions) {
    _numDimensions = numDimensions;
    _intBuffer = ByteBuffer.wrap(new byte[numDimensions * V1Constants.Numbers.INTEGER_SIZE]).asIntBuffer();
  }

  public DimensionBuffer(byte[] bytes) {
    _numDimensions = bytes.length / V1Constants.Numbers.INTEGER_SIZE;
    // NOTE: the byte array is returned from Unsafe which uses native order
    _intBuffer = ByteBuffer.wrap(bytes).order(NATIVE_ORDER).asIntBuffer();
  }

  public static DimensionBuffer fromBytes(byte[] bytes) {
    return new DimensionBuffer(bytes);
  }

  public int getDimension(int index) {
    return _intBuffer.get(index);
  }

  public void setDimension(int index, int value) {
    _intBuffer.put(index, value);
  }

  @SuppressWarnings({"EqualsWhichDoesntCheckParameterClass", "CheckStyle"})
  @Override
  public boolean equals(Object obj) {
    DimensionBuffer that = (DimensionBuffer) obj;
    for (int i = 0; i < _numDimensions; i++) {
      if (_intBuffer.get(i) != that._intBuffer.get(i)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("[");
    String delimiter = "";
    for (int i = 0; i < _numDimensions; i++) {
      builder.append(delimiter).append(_intBuffer.get(i));
      delimiter = ", ";
    }
    builder.append("]");
    return builder.toString();
  }
}

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

import java.nio.ByteBuffer;
import java.nio.IntBuffer;


public class DimensionBuffer {

  IntBuffer _intBuffer;

  public DimensionBuffer(int numDimensions) {
    this(ByteBuffer.wrap(new byte[numDimensions * Integer.SIZE / 8]).asIntBuffer());
  }

  public DimensionBuffer(IntBuffer intBuffer) {
    _intBuffer = intBuffer;
  }

  static DimensionBuffer fromBytes(byte[] byteArray) {
    return new DimensionBuffer(ByteBuffer.wrap(byteArray.clone()).asIntBuffer());
  }

  public void setDimension(int index, int value) {
    _intBuffer.put(index, value);
  }

  public int getDimension(int index) {
    return _intBuffer.get(index);
  }

  @Override
  public int hashCode() {
    return _intBuffer.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof DimensionBuffer) && _intBuffer.equals(((DimensionBuffer) obj)._intBuffer);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[");
    String delim = "";
    for (int i = 0; i < _intBuffer.limit(); i++) {
      sb.append(delim).append(_intBuffer.get(i));
      delim = ", ";
    }
    sb.append("]");
    return sb.toString();
  }
}

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
package com.linkedin.pinot.core.startree;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;


public class DimensionBuffer {

  IntBuffer buffer;
  private int numDimensions;

  public DimensionBuffer(int numDimensions) {
    this(numDimensions, ByteBuffer.wrap(new byte[numDimensions * 4]).asIntBuffer());
    this.numDimensions = numDimensions;
  }

  public DimensionBuffer(int numDimensions, IntBuffer intBuffer) {
    this.numDimensions = numDimensions;
    buffer = intBuffer;
  }

  static DimensionBuffer fromBytes(byte[] byteArray) {
    return new DimensionBuffer(byteArray.length / 4, ByteBuffer.wrap(byteArray).asIntBuffer());
  }

  public int getDimension(int index) {
    return buffer.get(index);
  }

  @Override
  public boolean equals(Object obj) {
    DimensionBuffer that = (DimensionBuffer) obj;
    for (int i = 0; i < numDimensions; i++) {
      if (buffer.get(i) != that.getDimension(i)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[");
    String delim = "";
    for (int i = 0; i < numDimensions; i++) {
      sb.append(delim).append(buffer.get(i));
      delim = ", ";
    }
    sb.append("]");
    return sb.toString();
  }

  public void setDimension(int index, int value) {
    buffer.put(index, value);
  }
}

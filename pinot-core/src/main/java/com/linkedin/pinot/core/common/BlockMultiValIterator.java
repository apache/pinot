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
package com.linkedin.pinot.core.common;

/**
 *
 *
 */
public abstract class BlockMultiValIterator implements BlockValIterator {

  public int nextCharVal(int[] charArray) {
    throw new UnsupportedOperationException();
  }

  public int nextIntVal(int[] intArray) {
    throw new UnsupportedOperationException();
  }

  public int nextLongVal(long[] longArray) {
    throw new UnsupportedOperationException();
  }

  public int nextFloatVal(float[] floatArray) {
    throw new UnsupportedOperationException();
  }

  public int nextDoubleVal(double[] doubleArray) {
    throw new UnsupportedOperationException();
  }

  public byte[][] nextBytesArrayVal(byte[][] bytesArrays) {
    throw new UnsupportedOperationException();

  }

}

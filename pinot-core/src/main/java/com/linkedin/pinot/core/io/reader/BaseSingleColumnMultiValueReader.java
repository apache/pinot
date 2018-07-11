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
package com.linkedin.pinot.core.io.reader;


public abstract class BaseSingleColumnMultiValueReader<T extends ReaderContext>
    implements SingleColumnMultiValueReader<T> {

  public int getCharArray(int row, char[] charArray) {
    throw new UnsupportedOperationException();
  }

  public int getShortArray(int row, short[] shortsArray) {
    throw new UnsupportedOperationException();
  }

  public int getIntArray(int row, int[] intArray) {
    throw new UnsupportedOperationException();
  }

  public int getLongArray(int row, long[] longArray) {
    throw new UnsupportedOperationException();
  }

  public int getFloatArray(int row, float[] floatArray) {
    throw new UnsupportedOperationException();
  }

  public int getDoubleArray(int row, double[] doubleArray) {
    throw new UnsupportedOperationException();
  }

  public int getStringArray(int row, String[] stringArray) {
    throw new UnsupportedOperationException();
  }

  public int getBytesArray(int row, byte[][] bytesArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T createContext() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntArray(int row, int[] intArray, T readerContext) {
    throw new UnsupportedOperationException();
  }

}

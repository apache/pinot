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
package com.linkedin.pinot.core.io.readerwriter;

import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.writer.SingleColumnMultiValueWriter;


public abstract class BaseSingleColumnMultiValueReaderWriter<T extends ReaderContext>
    implements SingleColumnMultiValueReader<T>, SingleColumnMultiValueWriter {

  @Override
  public int getCharArray(int row, char[] charArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getShortArray(int row, short[] shortsArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntArray(int row, int[] intArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getLongArray(int row, long[] longArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getFloatArray(int row, float[] floatArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getDoubleArray(int row, double[] doubleArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getStringArray(int row, String[] stringArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBytesArray(int row, byte[][] bytesArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T createContext() {
    return null;
  }

  @Override
  public int getIntArray(int row, int[] intArray, T readerContext) {
    return getIntArray(row, intArray);
  }

  @Override
  public void setCharArray(int row, char[] charArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setShortArray(int row, short[] shortsArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setIntArray(int row, int[] intArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLongArray(int row, long[] longArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFloatArray(int row, float[] floatArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDoubleArray(int row, double[] doubleArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setStringArray(int row, String[] stringArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBytesArray(int row, byte[][] bytesArray) {
    throw new UnsupportedOperationException();
  }
}

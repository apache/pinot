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
package com.linkedin.pinot.core.io.readerwriter;

import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.writer.SingleColumnSingleValueWriter;


public abstract class BaseSingleColumnSingleValueReaderWriter<T extends ReaderContext>
    implements SingleColumnSingleValueReader<T>, SingleColumnSingleValueWriter {

  @Override
  public char getChar(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShort(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int row, ReaderContext context) {
    return getInt(row);
  }

  @Override
  public long getLong(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int row, ReaderContext context) {
    return getLong(row);
  }

  @Override
  public float getFloat(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int row, ReaderContext context) {
    return getFloat(row);
  }

  @Override
  public double getDouble(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int row, ReaderContext context) {
    return getDouble(row);
  }

  @Override
  public String getString(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(int row, ReaderContext context) {
    return getString(row);
  }

  @Override
  public byte[] getBytes(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T createContext() {
    return null;
  }

  @Override
  public void setChar(int row, char ch) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInt(int row, int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setShort(int row, short s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLong(int row, long l) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFloat(int row, float f) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDouble(int row, double d) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setString(int row, String string) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBytes(int row, byte[] bytes) {
    throw new UnsupportedOperationException();
  }
}

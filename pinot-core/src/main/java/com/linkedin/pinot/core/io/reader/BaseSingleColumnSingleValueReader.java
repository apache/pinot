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
package com.linkedin.pinot.core.io.reader;

import java.io.IOException;

public abstract class BaseSingleColumnSingleValueReader<T extends ReaderContext>
    implements SingleColumnSingleValueReader<T> {
 

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();
  }

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
  public int getInt(int rowId, T context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int rowId, T context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int rowId, T context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int rowId, T context) {
    throw new UnsupportedOperationException();
  }

  public String getString(int row) {
    throw new UnsupportedOperationException();
  }

  public String getString(int row, T context) {
    throw new UnsupportedOperationException();
  }

  public byte[] getBytes(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos) {
    throw new UnsupportedOperationException("not supported");
  }
}

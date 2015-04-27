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
package com.linkedin.pinot.core.index.readerwriter.impl;

import java.io.IOException;

import com.linkedin.pinot.core.index.reader.DataFileMetadata;
import com.linkedin.pinot.core.index.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.index.writer.SingleColumnSingleValueWriter;


public class FixedByteSingleColumnSingleValueReaderWriter implements SingleColumnSingleValueReader,
    SingleColumnSingleValueWriter {

  /**
   *
   * @param rows
   * @param column
   * @param columnSize
   */
  public FixedByteSingleColumnSingleValueReaderWriter(int rows, int column, int[] columnSizesInBytes) {
  }

  @Override
  public DataFileMetadata getMetadata() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean setMetadata(DataFileMetadata metadata) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void setChar(int row, char ch) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setInt(int row, int i) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setShort(int row, short s) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setLong(int row, int l) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setFloat(int row, float f) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setDouble(int row, double d) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setString(int row, String string) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBytes(int row, byte[] bytes) {
    // TODO Auto-generated method stub

  }

  @Override
  public char getChar(int row) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public short getShort(int row) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getInt(int row) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getLong(int row) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public float getFloat(int row) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public double getDouble(int row) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getString(int row) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] getBytes(int row) {
    // TODO Auto-generated method stub
    return null;
  }

}

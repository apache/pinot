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
import com.linkedin.pinot.core.index.readerwriter.SingleColumnMultiValueReaderWriter;


public class FixedByteSingleColumnMultiValueReaderWriter implements SingleColumnMultiValueReaderWriter {

  private static final int MAX_NUMBER_OF_MULTIVALUES = 2000;

  /**
   * number of columns is 1, column size is variable, data type is always int
   * @param rows
   */

  public FixedByteSingleColumnMultiValueReaderWriter(int rows, int columnSizeInBytes, int maxNumberOfMultiValues) {
  }

  @Override
  public int getCharArray(int row, char[] charArray) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getShortArray(int row, short[] shortsArray) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getIntArray(int row, int[] intArray) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getLongArray(int row, long[] longArray) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getFloatArray(int row, float[] floatArray) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getDoubleArray(int row, double[] doubleArray) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getStringArray(int row, String[] stringArray) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getBytesArray(int row, byte[][] bytesArray) {
    // TODO Auto-generated method stub
    return 0;
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
  public void setCharArray(int row, char[] charArray) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setShortArray(int row, short[] shortsArray) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setIntArray(int row, int[] intArray) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setLongArray(int row, long[] longArray) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setFloatArray(int row, float[] floatArray) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setDoubleArray(int row, double[] doubleArray) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setStringArray(int row, String[] stringArray) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBytesArray(int row, byte[][] bytesArray) {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean setMetadata(DataFileMetadata metadata) {
    // TODO Auto-generated method stub
    return false;
  }

}

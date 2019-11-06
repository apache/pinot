/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.io.readerwriter.impl;

import java.io.IOException;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.io.readerwriter.BaseSingleColumnSingleValueReaderWriter;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.core.io.writer.impl.MutableOffHeapByteArrayStore;

public class VarByteSingleColumnSingleValueReaderWriter extends BaseSingleColumnSingleValueReaderWriter {
  private final MutableOffHeapByteArrayStore _byteArrayStore;
  private int _lengthOfShortestElement;
  private int _lengthOfLongestElement;

  public VarByteSingleColumnSingleValueReaderWriter(
      PinotDataBufferMemoryManager memoryManager,
      String allocationContext,
      int estimatedMaxNumberOfValues,
      int estimatedAverageStringLength) {
    _byteArrayStore = new MutableOffHeapByteArrayStore(memoryManager, allocationContext, estimatedMaxNumberOfValues, estimatedAverageStringLength);
    _lengthOfShortestElement = Integer.MAX_VALUE;
    _lengthOfLongestElement = Integer.MIN_VALUE;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _lengthOfShortestElement;
  }

  @Override
  public int getLengthOfLongestElement() {
    return _lengthOfLongestElement;
  }

  @Override
  public void close()
      throws IOException {
    _byteArrayStore.close();
  }

  @Override
  public void setInt(int row, int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLong(int row, long l) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFloat(int row, float f) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDouble(int row, double d) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setString(int row, String val) {
    byte[] serializedValue = StringUtil.encodeUtf8(val);
    setBytes(row, serializedValue);
  }

  @Override
  public String getString(int row) {
    return StringUtil.decodeUtf8(_byteArrayStore.get(row));
  }

  @Override
  public void setBytes(int row, byte[] value) {
    _byteArrayStore.add(value);
    _lengthOfLongestElement = Math.max(_lengthOfLongestElement, value.length);
    _lengthOfShortestElement = Math.min(_lengthOfShortestElement, value.length);
  }

  @Override
  public byte[] getBytes(int row) {
    return _byteArrayStore.get(row);
  }
}
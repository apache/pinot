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
package org.apache.pinot.segment.local.realtime.impl.forward;

import java.io.IOException;
import org.apache.pinot.segment.local.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.MutableOffHeapByteArrayStore;
import org.apache.pinot.segment.spi.index.reader.MutableForwardIndex;
import org.apache.pinot.spi.data.FieldSpec.DataType;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Single-value forward index reader-writer for variable length values (STRING and BYTES).
 */
public class VarByteSVMutableForwardIndex implements MutableForwardIndex {
  private final DataType _valueType;
  private final MutableOffHeapByteArrayStore _byteArrayStore;
  private int _lengthOfShortestElement;
  private int _lengthOfLongestElement;

  public VarByteSVMutableForwardIndex(DataType valueType, PinotDataBufferMemoryManager memoryManager,
      String allocationContext, int estimatedMaxNumberOfValues, int estimatedAverageStringLength) {
    _valueType = valueType;
    _byteArrayStore = new MutableOffHeapByteArrayStore(memoryManager, allocationContext, estimatedMaxNumberOfValues,
        estimatedAverageStringLength);
    _lengthOfShortestElement = Integer.MAX_VALUE;
    _lengthOfLongestElement = Integer.MIN_VALUE;
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public DataType getValueType() {
    return _valueType;
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
  public String getString(int docId) {
    return new String(_byteArrayStore.get(docId), UTF_8);
  }

  @Override
  public byte[] getBytes(int docId) {
    return _byteArrayStore.get(docId);
  }

  @Override
  public void setString(int docId, String value) {
    setBytes(docId, value.getBytes(UTF_8));
  }

  @Override
  public void setBytes(int docId, byte[] value) {
    _byteArrayStore.add(value);
    _lengthOfLongestElement = Math.max(_lengthOfLongestElement, value.length);
    _lengthOfShortestElement = Math.min(_lengthOfShortestElement, value.length);
  }

  @Override
  public void close()
      throws IOException {
    _byteArrayStore.close();
  }
}

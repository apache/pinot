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
package org.apache.pinot.segment.local.utils;

import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.IOException;
import java.util.Arrays;
import org.apache.pinot.segment.local.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.local.io.readerwriter.impl.FixedByteSingleValueMultiColumnReaderWriter;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BaseOffHeapMutableDictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.FixedIntArray;


/**
 * Implementation of {@link IdMap} with {@link FixedIntArray} as key.
 *
 * This implementation extends the {@link BaseOffHeapMutableDictionary} for code-reuse of off-heap functionality. The
 * dictionary related APIs are not supported.
 */
public class FixedIntArrayOffHeapIdMap extends BaseOffHeapMutableDictionary implements IdMap<FixedIntArray> {
  private final FixedByteSingleValueMultiColumnReaderWriter _dictIdToValue;
  private final int _numColumns;

  public FixedIntArrayOffHeapIdMap(int estimatedCardinality, int maxOverflowHashSize, int numColumns,
      PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    super(estimatedCardinality, maxOverflowHashSize, memoryManager, allocationContext);

    int initialSize = nearestPowerOf2(estimatedCardinality);
    int[] columnSizesInBytes = new int[numColumns];
    Arrays.fill(columnSizesInBytes, Integer.BYTES);

    _dictIdToValue = new FixedByteSingleValueMultiColumnReaderWriter(initialSize, columnSizesInBytes, memoryManager,
        allocationContext);
    _numColumns = numColumns;
  }

  @Override
  public int put(FixedIntArray key) {
    return indexValue(key, null);
  }

  @Override
  public int getId(FixedIntArray key) {
    return getDictId(key, null);
  }

  @Override
  public FixedIntArray getKey(int id) {
    int[] value = new int[_numColumns];
    for (int i = 0; i < _numColumns; i++) {
      value[i] = _dictIdToValue.getInt(id, i);
    }
    return new FixedIntArray(value);
  }

  @Override
  public int size() {
    return length();
  }

  @Override
  protected void setValue(int dictId, Object value, byte[] serializedValue) {
    FixedIntArray intArray = (FixedIntArray) value;
    int[] values = intArray.elements();
    for (int i = 0; i < _numColumns; i++) {
      _dictIdToValue.setInt(dictId, i, values[i]);
    }
  }

  @Override
  protected boolean equalsValueAt(int dictId, Object value, byte[] serializedValue) {
    return getKey(dictId).equals(value);
  }

  @Override
  public void doClose()
      throws IOException {
    _dictIdToValue.close();
  }

  @Override
  public int index(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] index(Object[] values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compare(int dictId1, int dictId2) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Comparable getMinVal() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Comparable getMaxVal() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getSortedValues() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataType getValueType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int indexOf(String stringValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLongValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloatValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDoubleValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getStringValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getAvgValueSize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getTotalOffHeapMemUsed() {
    throw new UnsupportedOperationException();
  }
}

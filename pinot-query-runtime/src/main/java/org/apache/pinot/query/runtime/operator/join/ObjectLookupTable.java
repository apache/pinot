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
package org.apache.pinot.query.runtime.operator.join;

import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.core.data.table.Key;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code DoubleLookupTable} is a lookup table for non-primitive keys.
 */
@SuppressWarnings("unchecked")
public class ObjectLookupTable extends LookupTable {
  private final Map<Object, Object> _lookupTable = Maps.newHashMapWithExpectedSize(INITIAL_CAPACITY);

  private static Object[] toKeys(Object[] keyColumns) {
    Object[][] boxedColumns = new Object[keyColumns.length][];
    for (int i = 0; i < keyColumns.length; i++) {
      if (keyColumns[i] instanceof long[]) {
        boxedColumns[i] = Arrays.stream((long[]) keyColumns[i]).boxed().toArray(Long[]::new);
      } else if (keyColumns[i] instanceof int[]) {
        boxedColumns[i] = Arrays.stream((int[]) keyColumns[i]).boxed().toArray(Integer[]::new);
      } else if (keyColumns[i] instanceof float[]) {
        float[] floatArray = (float[]) keyColumns[i];
        boxedColumns[i] = new Float[floatArray.length];
        for (int j = 0; j < floatArray.length; j++) {
          boxedColumns[i][j] = floatArray[j];
        }
      } else if (keyColumns[i] instanceof double[]) {
        boxedColumns[i] = Arrays.stream((double[]) keyColumns[i]).boxed().toArray(Double[]::new);
      } else {
        boxedColumns[i] = (Object[]) keyColumns[i];
      }
    }
    Object[] keys;
    if (boxedColumns.length == 1) {
      keys = boxedColumns[0];
    } else {
      keys = new Object[boxedColumns[0].length];
      for (int rowId = 0; rowId < boxedColumns[0].length; rowId++) {
        Object[] keyValues = new Object[boxedColumns.length];
        for (int columnId = 0; columnId < boxedColumns.length; columnId++) {
          keyValues[columnId] = boxedColumns[columnId][rowId];
        }
        keys[rowId] = new Key(keyValues);
      }
    }
    return keys;
  }

  @Override
  public RoaringBitmap matches(Object[] keyColumns, boolean invert) {
    RoaringBitmap result = new RoaringBitmap();
    Object[] keys = toKeys(keyColumns);
    for (int i = 0; i < keys.length; i++) {
      if (!invert == _lookupTable.containsKey(keys[i])) {
        result.add(i);
      }
    }
    return result;
  }

  @Override
  public Object[] getAll(Object[] keyColumns, @Nullable Map<Object, BitSet> record, BitSet placeholder) {
    Object[] keys = toKeys(keyColumns);
    Object[] result = new Object[keys.length];
    for (int rowId = 0; rowId < keys.length; rowId++) {
      result[rowId] = _lookupTable.get(keys[rowId]);
      if (record != null) {
        record.put(keys[rowId], placeholder);
      }
    }
    return result;
  }


  @Override
  public String toString() {
    return _lookupTable.toString();
  }

  @Override
  public void addRow(@Nullable Object key, Object[] row) {
    _lookupTable.compute(key, (k, v) -> computeNewValue(row, v));
  }

  @Override
  public void finish() {
    if (!_keysUnique) {
      for (Map.Entry<Object, Object> entry : _lookupTable.entrySet()) {
        convertValueToList(entry);
      }
    }
  }

  @Override
  public boolean containsKey(@Nullable Object key) {
    return _lookupTable.containsKey(key);
  }

  @Nullable
  @Override
  public Object lookup(@Nullable Object key) {
    return _lookupTable.get(key);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Set<Map.Entry<Object, Object>> entrySet() {
    return _lookupTable.entrySet();
  }
}

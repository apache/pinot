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

import it.unimi.dsi.fastutil.floats.Float2ObjectMap;
import it.unimi.dsi.fastutil.floats.Float2ObjectOpenHashMap;
import java.util.BitSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code FloatLookupTable} is a lookup table for float keys.
 */
@SuppressWarnings("unchecked")
public class FloatLookupTable extends PrimitiveLookupTable {
  private final Float2ObjectOpenHashMap<Object> _lookupTable = new Float2ObjectOpenHashMap<>(INITIAL_CAPACITY);

  @Override
  public RoaringBitmap matches(Object[] keyColumns, boolean invert) {
    assert keyColumns.length == 1;
    float[] keys = (float[]) keyColumns[0];
    RoaringBitmap result = new RoaringBitmap();
    for (int rowId = 0; rowId < keys.length; rowId++) {
      if (!invert == _lookupTable.containsKey(keys[rowId])) {
        result.add(rowId);
      }
    }
    return result;
  }

  @Override
  public Object[] getAll(Object[] keyColumns, @Nullable Map<Object, BitSet> record, BitSet placeholder) {
    assert keyColumns.length == 1;
    float[] keys = (float[]) keyColumns[0];
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
  public void addRowNotNullKey(Object key, Object[] row) {
    _lookupTable.compute((float) key, (k, v) -> computeNewValue(row, v));
  }

  @Override
  public void finishNotNullKey() {
    if (!_keysUnique) {
      for (Float2ObjectMap.Entry<Object> entry : _lookupTable.float2ObjectEntrySet()) {
        convertValueToList(entry);
      }
    }
  }

  @Override
  public boolean containsNotNullKey(Object key) {
    return _lookupTable.containsKey((float) key);
  }

  @Nullable
  @Override
  public Object lookupNotNullKey(Object key) {
    return _lookupTable.get((float) key);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Set<Map.Entry<Object, Object>> notNullKeyEntrySet() {
    return (Set) _lookupTable.float2ObjectEntrySet();
  }
}

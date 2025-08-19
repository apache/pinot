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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Map;
import java.util.Set;


/**
 * The {@code IntLookupTable} is a lookup table for int keys.
 */
@SuppressWarnings("unchecked")
public class IntLookupTable extends PrimitiveLookupTable {
  private final Int2ObjectOpenHashMap<Object> _lookupTable = new Int2ObjectOpenHashMap<>(INITIAL_CAPACITY);

  @Override
  protected void addRowNotNullKey(Object key, Object[] row) {
    _lookupTable.compute((int) key, (k, v) -> computeNewValue(row, v));
  }

  @Override
  protected void finishNotNullKey() {
    for (Int2ObjectMap.Entry<Object> entry : _lookupTable.int2ObjectEntrySet()) {
      convertValueToList(entry);
    }
  }

  @Override
  protected boolean containsNotNullKey(Object key) {
    return _lookupTable.containsKey((int) key);
  }

  @Override
  protected Object lookupNotNullKey(Object key) {
    return _lookupTable.get((int) key);
  }

  @Override
  protected Set<Map.Entry<Object, Object>> notNullKeyEntrySet() {
    return (Set) _lookupTable.int2ObjectEntrySet();
  }

  @Override
  public int size() {
    return _lookupTable.size();
  }
}

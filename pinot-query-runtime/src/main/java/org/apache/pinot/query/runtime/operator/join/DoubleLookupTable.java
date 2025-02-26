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

import it.unimi.dsi.fastutil.doubles.Double2ObjectMap;
import it.unimi.dsi.fastutil.doubles.Double2ObjectOpenHashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;


/**
 * The {@code DoubleLookupTable} is a lookup table for double keys.
 */
@SuppressWarnings("unchecked")
public class DoubleLookupTable extends LookupTable {
  private final Double2ObjectOpenHashMap<Object> _lookupTable = new Double2ObjectOpenHashMap<>(INITIAL_CAPACITY);

  @Override
  public void addRow(Object key, Object[] row) {
    _lookupTable.compute((double) key, (k, v) -> computeNewValue(row, v));
  }

  @Override
  public void finish() {
    if (!_keysUnique) {
      for (Double2ObjectMap.Entry<Object> entry : _lookupTable.double2ObjectEntrySet()) {
        convertValueToList(entry);
      }
    }
  }

  @Override
  public boolean containsKey(Object key) {
    return _lookupTable.containsKey((double) key);
  }

  @Nullable
  @Override
  public Object lookup(Object key) {
    return _lookupTable.get((double) key);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Set<Map.Entry> entrySet() {
    return (Set) _lookupTable.double2ObjectEntrySet();
  }
}

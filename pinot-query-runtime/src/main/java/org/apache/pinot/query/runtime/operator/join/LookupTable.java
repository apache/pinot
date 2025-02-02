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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;


public abstract class LookupTable {
  // TODO: Make it configurable
  protected static final int INITIAL_CAPACITY = 10000;

  protected boolean _keysUnique = true;

  /**
   * Adds a row to the lookup table.
   */
  public abstract void addRow(Object key, Object[] row);

  @SuppressWarnings("unchecked")
  protected Object calculateValue(Object[] row, @Nullable Object currentValue) {
    if (currentValue == null) {
      return row;
    } else {
      _keysUnique = false;
      if (currentValue instanceof List) {
        ((List<Object[]>) currentValue).add(row);
        return currentValue;
      } else {
        List<Object[]> rows = new ArrayList<>();
        rows.add((Object[]) currentValue);
        rows.add(row);
        return rows;
      }
    }
  }

  /**
   * Finishes adding rows to the lookup table. This method should be called after all rows are added to the lookup
   * table, and before looking up rows.
   */
  public abstract void finish();

  protected static void convertValueToList(Map.Entry<?, Object> entry) {
    Object value = entry.getValue();
    if (value instanceof Object[]) {
      entry.setValue(Collections.singletonList(value));
    }
  }

  /**
   * Returns {@code true} when all the keys added to the lookup table are unique.
   * When all keys are unique, the value of the lookup table is a single row ({@code Object[]}). When keys are not
   * unique, the value of the lookup table is a list of rows ({@code List<Object[]>}).
   */
  public boolean isKeysUnique() {
    return _keysUnique;
  }

  /**
   * Returns {@code true} if the lookup table contains the given key.
   */
  public abstract boolean containsKey(Object key);

  /**
   * Returns the row/rows for the given key. When {@link #isKeysUnique} returns {@code true}, this method returns a
   * single row ({@code Object[]}). When {@link #isKeysUnique} returns {@code false}, this method returns a list of rows
   * ({@code List<Object[]>}). Returns {@code null} if the key does not exist in the lookup table.
   */
  @Nullable
  public abstract Object lookup(Object key);

  /**
   * Returns all the entries in the lookup table. When {@link #isKeysUnique} returns {@code true}, the value of the
   * entries is a single row ({@code Object[]}). When {@link #isKeysUnique} returns {@code false}, the value of the
   * entries is a list of rows ({@code List<Object[]>}).
   */
  @SuppressWarnings("rawtypes")
  public abstract Set<Map.Entry> entrySet();
}

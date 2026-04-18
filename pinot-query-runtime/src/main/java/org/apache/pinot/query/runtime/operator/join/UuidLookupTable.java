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
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.UuidUtils.UuidKey;


/**
 * Lookup table optimized for Pinot's logical UUID type.
 */
@SuppressWarnings("unchecked")
public class UuidLookupTable extends LookupTable {
  private final Map<Object, Object> _lookupTable = Maps.newHashMapWithExpectedSize(INITIAL_CAPACITY);

  @Override
  public void addRow(@Nullable Object key, Object[] row) {
    Object normalizedKey = normalizeKey(key);
    if (normalizedKey == null) {
      return;
    }
    _lookupTable.compute(normalizedKey, (k, v) -> computeNewValue(row, v));
  }

  @Override
  public void finish() {
    if (!_keysUnique) {
      for (Map.Entry<Object, Object> entry : _lookupTable.entrySet()) {
        convertValueToList(entry);
      }
    }
  }

  @Nullable
  @Override
  public Object normalizeKey(@Nullable Object key) {
    return key != null ? UuidKey.fromObject(key) : null;
  }

  @Override
  public boolean containsKey(@Nullable Object key) {
    Object normalizedKey = normalizeKey(key);
    return normalizedKey != null && _lookupTable.containsKey(normalizedKey);
  }

  @Nullable
  @Override
  public Object lookup(@Nullable Object key) {
    Object normalizedKey = normalizeKey(key);
    return normalizedKey != null ? _lookupTable.get(normalizedKey) : null;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Set<Map.Entry<Object, Object>> entrySet() {
    return _lookupTable.entrySet();
  }

  @Override
  public int size() {
    return _lookupTable.size();
  }
}

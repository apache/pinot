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

import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;


public abstract class PrimitiveLookupTable extends LookupTable {

  private Object _valueForNullKey;

  @Override
  public void addRow(@Nullable Object key, Object[] row) {
    if (key == null) {
      _valueForNullKey = computeNewValue(row, _valueForNullKey);
      return;
    }
    addRowNotNullKey(key, row);
  }

  @Override
  public void finish() {
    if (!_keysUnique) {
      if (_valueForNullKey != null) {
        _valueForNullKey = convertValueToList(_valueForNullKey);
      }
      finishNotNullKey();
    }
  }

  protected abstract void finishNotNullKey();

  protected abstract void addRowNotNullKey(Object key, Object[] row);

  @Override
  public boolean containsKey(@Nullable Object key) {
    if (key == null) {
      return _valueForNullKey != null;
    }
    return containsNotNullKey(key);
  }

  protected abstract boolean containsNotNullKey(Object key);

  @Nullable
  @Override
  public Object lookup(@Nullable Object key) {
    if (key == null) {
      return _valueForNullKey;
    }
    return lookupNotNullKey(key);
  }

  protected abstract Object lookupNotNullKey(Object key);

  @SuppressWarnings("rawtypes")
  @Override
  public Set<Map.Entry<Object, Object>> entrySet() {
    Set<Map.Entry<Object, Object>> notNullSet = notNullKeyEntrySet();
    if (_valueForNullKey != null) {
      Set<Map.Entry<Object, Object>> nullEntry = Set.of(new Map.Entry<>() {
        @Override
        public Object getKey() {
          return null;
        }

        @Override
        public Object getValue() {
          return _valueForNullKey;
        }

        @Override
        public Object setValue(Object value) {
          throw new UnsupportedOperationException();
        }
      });
      return Sets.union(notNullSet, nullEntry);
    } else {
      return notNullSet;
    }
  }

  protected abstract Set<Map.Entry<Object, Object>> notNullKeyEntrySet();
}

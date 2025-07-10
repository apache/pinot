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

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;


public abstract class PrimitiveLookupTable extends LookupTable {

  @Override
  public void addRow(@Nullable Object key, Object[] row) {
    if (key == null) {
      // Do nothing: SQL semantics ignore null keys
      return;
    }
    addRowNotNullKey(key, row);
  }

  @Override
  public void finish() {
    if (!_keysUnique) {
      finishNotNullKey();
    }
  }

  protected abstract void finishNotNullKey();

  protected abstract void addRowNotNullKey(Object key, Object[] row);

  @Override
  public boolean containsKey(@Nullable Object key) {
    if (key == null) {
      // SQL semantics: null never matches
      return false;
    }
    return containsNotNullKey(key);
  }

  protected abstract boolean containsNotNullKey(Object key);

  @Nullable
  @Override
  public Object lookup(@Nullable Object key) {
    if (key == null) {
      // SQL semantics: null lookup returns null
      return null;
    }
    return lookupNotNullKey(key);
  }

  protected abstract Object lookupNotNullKey(Object key);

  @SuppressWarnings("rawtypes")
  @Override
  public Set<Map.Entry<Object, Object>> entrySet() {
    return notNullKeyEntrySet();
  }

  protected abstract Set<Map.Entry<Object, Object>> notNullKeyEntrySet();
}

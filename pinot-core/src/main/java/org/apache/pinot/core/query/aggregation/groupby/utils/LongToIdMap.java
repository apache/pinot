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
package org.apache.pinot.core.query.aggregation.groupby.utils;

import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;


/**
 * Implementation of {@link ValueToIdMap} for long.
 */
public class LongToIdMap extends BaseValueToIdMap {
  Long2IntMap _valueToIdMap;
  LongList _idToValueMap;

  public LongToIdMap() {
    _valueToIdMap = new Long2IntOpenHashMap();
    _valueToIdMap.defaultReturnValue(INVALID_KEY);
    _idToValueMap = new LongArrayList();
  }

  @Override
  public int put(long value) {
    int id = _valueToIdMap.get(value);
    if (id == INVALID_KEY) {
      id = _idToValueMap.size();
      _valueToIdMap.put(value, id);
      _idToValueMap.add(value);
    }
    return id;
  }

  @Override
  public long getLong(int id) {
    assert id < _idToValueMap.size();
    return _idToValueMap.getLong(id);
  }

  @Override
  public String getString(int id) {
    return Long.toString(getLong(id));
  }
}

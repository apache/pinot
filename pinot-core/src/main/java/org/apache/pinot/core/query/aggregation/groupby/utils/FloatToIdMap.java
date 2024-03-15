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

import it.unimi.dsi.fastutil.floats.Float2IntOpenHashMap;
import it.unimi.dsi.fastutil.floats.FloatArrayList;


/**
 * Implementation of {@link ValueToIdMap} for float.
 */
public class FloatToIdMap implements ValueToIdMap {
  private final Float2IntOpenHashMap _valueToIdMap;
  private final FloatArrayList _idToValueMap;

  public FloatToIdMap() {
    _valueToIdMap = new Float2IntOpenHashMap();
    _valueToIdMap.defaultReturnValue(INVALID_KEY);
    _idToValueMap = new FloatArrayList();
  }

  @Override
  public int put(float value) {
    int numValues = _valueToIdMap.size();
    int id = _valueToIdMap.computeIfAbsent(value, k -> numValues);
    if (id == numValues) {
      _idToValueMap.add(value);
    }
    return id;
  }

  @Override
  public int put(Object value) {
    return put((float) value);
  }

  @Override
  public int getId(float value) {
    return _valueToIdMap.get(value);
  }

  @Override
  public int getId(Object value) {
    return getId((float) value);
  }

  @Override
  public Float get(int id) {
    return _idToValueMap.getFloat(id);
  }
}

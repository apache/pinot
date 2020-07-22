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

import it.unimi.dsi.fastutil.doubles.Double2IntMap;
import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;


/**
 * Implementation of {@link ValueToIdMap} for double.
 */
public class DoubleToIdMap extends BaseValueToIdMap {
  Double2IntMap _valueToIdMap;
  DoubleList _idToValueMap;

  public DoubleToIdMap() {
    _valueToIdMap = new Double2IntOpenHashMap();
    _valueToIdMap.defaultReturnValue(INVALID_KEY);
    _idToValueMap = new DoubleArrayList();
  }

  @Override
  public int put(double value) {
    int id = _valueToIdMap.get(value);
    if (id == INVALID_KEY) {
      id = _idToValueMap.size();
      _valueToIdMap.put(value, id);
      _idToValueMap.add(value);
    }
    return id;
  }

  @Override
  public double getDouble(int id) {
    assert id < _idToValueMap.size();
    return _idToValueMap.getDouble(id);
  }

  @Override
  public String getString(int id) {
    return Double.toString(getDouble(id));
  }
}

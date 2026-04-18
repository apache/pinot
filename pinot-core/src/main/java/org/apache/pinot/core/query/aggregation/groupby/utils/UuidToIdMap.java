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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UuidUtils.UuidKey;


/**
 * Implementation of {@link ValueToIdMap} for Pinot's logical UUID type.
 */
public class UuidToIdMap implements ValueToIdMap {
  private final Object2IntOpenHashMap<UuidKey> _valueToIdMap;
  private final ArrayList<ByteArray> _idToValueMap;

  public UuidToIdMap() {
    _valueToIdMap = new Object2IntOpenHashMap<>();
    _valueToIdMap.defaultReturnValue(INVALID_KEY);
    _idToValueMap = new ArrayList<>();
  }

  @Override
  public int put(Object value) {
    UuidKey uuidKey = UuidKey.fromObject(value);
    int numValues = _valueToIdMap.size();
    int id = _valueToIdMap.computeIntIfAbsent(uuidKey, ignored -> numValues);
    if (id == numValues) {
      _idToValueMap.add(uuidKey.toByteArray());
    }
    return id;
  }

  @Override
  public int getId(Object value) {
    return _valueToIdMap.getInt(UuidKey.fromObject(value));
  }

  @Override
  public Object get(int id) {
    return _idToValueMap.get(id);
  }
}

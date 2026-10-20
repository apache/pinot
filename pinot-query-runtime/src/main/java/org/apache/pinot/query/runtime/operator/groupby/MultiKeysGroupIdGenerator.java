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
package org.apache.pinot.query.runtime.operator.groupby;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Iterator;
import java.util.function.ToIntFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.groupby.utils.ValueToIdMap;
import org.apache.pinot.core.query.aggregation.groupby.utils.ValueToIdMapFactory;
import org.apache.pinot.spi.utils.FixedIntArray;


/// [GroupIdGenerator] for 3 or more key columns, where each key value is converted into a key id and the resulting
/// key ids are wrapped into a [FixedIntArray] used as the map key.
///
/// This class is not thread-safe. It keeps a reusable [FixedIntArray] to look up the group id without allocating on
/// every row, so a single instance must be accessed by a single thread at a time.
public class MultiKeysGroupIdGenerator implements GroupIdGenerator {
  private final Object2IntOpenHashMap<FixedIntArray> _groupIdMap;
  /// A function to generate the next group ID based on the current size of the map.
  /// We use this instead of a simple lambda to avoid capturing `this` and therefore allocate on each getGroupId call
  private final ToIntFunction<FixedIntArray> _groupIdGenerator;
  private final ValueToIdMap[] _keyToIdMaps;
  private final int _numGroupsLimit;

  /// Reusable key holding the key ids of the row being processed. It is replaced with a new instance whenever it is
  /// inserted into [#_groupIdMap], which takes ownership of the key.
  private FixedIntArray _currentKey;

  public MultiKeysGroupIdGenerator(ColumnDataType[] keyTypes, int numKeyColumns,
      int numGroupsLimit, int initialCapacity) {
    _groupIdMap = new Object2IntOpenHashMap<>(initialCapacity);
    _groupIdMap.defaultReturnValue(INVALID_ID);
    _groupIdGenerator = k -> _groupIdMap.size();

    _keyToIdMaps = new ValueToIdMap[numKeyColumns];
    for (int i = 0; i < numKeyColumns; i++) {
      _keyToIdMaps[i] = ValueToIdMapFactory.get(keyTypes[i].toDataType());
    }
    _numGroupsLimit = numGroupsLimit;
    _currentKey = new FixedIntArray(new int[numKeyColumns]);
  }

  @Override
  public int getGroupId(Object key) {
    Object[] keyValues = (Object[]) key;
    int numKeyColumns = _keyToIdMaps.length;
    FixedIntArray currentKey = _currentKey;
    int[] keyIds = currentKey.elements();
    int numGroups = _groupIdMap.size();
    if (numGroups < _numGroupsLimit) {
      for (int i = 0; i < numKeyColumns; i++) {
        Object keyValue = keyValues[i];
        keyIds[i] = keyValue != null ? _keyToIdMaps[i].put(keyValue) : NULL_ID;
      }
      int groupId = _groupIdMap.computeIfAbsent(currentKey, _groupIdGenerator);
      if (_groupIdMap.size() != numGroups) {
        // A new group is created and the map holds a reference to the current key, so allocate a new one
        _currentKey = new FixedIntArray(new int[numKeyColumns]);
      }
      return groupId;
    } else {
      for (int i = 0; i < numKeyColumns; i++) {
        Object keyValue = keyValues[i];
        if (keyValue == null) {
          keyIds[i] = NULL_ID;
        } else {
          int keyId = _keyToIdMaps[i].getId(keyValue);
          if (keyId == INVALID_ID) {
            return INVALID_ID;
          }
          keyIds[i] = keyId;
        }
      }
      return _groupIdMap.getInt(currentKey);
    }
  }

  @Override
  public int getNumGroups() {
    return _groupIdMap.size();
  }

  @Override
  public Iterator<GroupKey> getGroupKeyIterator(int numColumns) {
    return new Iterator<GroupKey>() {
      final ObjectIterator<Object2IntOpenHashMap.Entry<FixedIntArray>> _entryIterator =
          _groupIdMap.object2IntEntrySet().fastIterator();

      @Override
      public boolean hasNext() {
        return _entryIterator.hasNext();
      }

      @Override
      public GroupKey next() {
        Object2IntOpenHashMap.Entry<FixedIntArray> entry = _entryIterator.next();
        int[] keyIds = entry.getKey().elements();
        Object[] row = new Object[numColumns];
        int numKeyColumns = keyIds.length;
        for (int i = 0; i < numKeyColumns; i++) {
          int keyId = keyIds[i];
          if (keyId != NULL_ID) {
            row[i] = _keyToIdMaps[i].get(keyId);
          }
        }
        return new GroupKey(entry.getIntValue(), row);
      }
    };
  }
}

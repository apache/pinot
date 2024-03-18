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

import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Iterator;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.groupby.utils.ValueToIdMap;
import org.apache.pinot.core.query.aggregation.groupby.utils.ValueToIdMapFactory;


public class TwoKeysGroupIdGenerator implements GroupIdGenerator {
  private final Long2IntOpenHashMap _groupIdMap;
  private final ValueToIdMap _firstKeyToIdMap;
  private final ValueToIdMap _secondKeyToIdMap;
  private final int _numGroupsLimit;

  public TwoKeysGroupIdGenerator(ColumnDataType firstKeyType, ColumnDataType secondKeyType, int numGroupsLimit) {
    _groupIdMap = new Long2IntOpenHashMap();
    _groupIdMap.defaultReturnValue(INVALID_ID);
    _firstKeyToIdMap = ValueToIdMapFactory.get(firstKeyType.toDataType());
    _secondKeyToIdMap = ValueToIdMapFactory.get(secondKeyType.toDataType());
    _numGroupsLimit = numGroupsLimit;
  }

  @Override
  public int getGroupId(Object key) {
    Object[] keyValues = (Object[]) key;
    Object firstKey = keyValues[0];
    Object secondKey = keyValues[1];
    int numGroups = _groupIdMap.size();
    if (numGroups < _numGroupsLimit) {
      int firstKeyId = firstKey != null ? _firstKeyToIdMap.put(firstKey) : NULL_ID;
      int secondKeyId = secondKey != null ? _secondKeyToIdMap.put(secondKey) : NULL_ID;
      long longKey = ((long) firstKeyId << 32) | (secondKeyId & 0xFFFFFFFFL);
      return _groupIdMap.computeIfAbsent(longKey, k -> numGroups);
    } else {
      int firstKeyId;
      if (firstKey != null) {
        firstKeyId = _firstKeyToIdMap.getId(firstKey);
        if (firstKeyId == INVALID_ID) {
          return INVALID_ID;
        }
      } else {
        firstKeyId = NULL_ID;
      }
      int secondKeyId;
      if (secondKey != null) {
        secondKeyId = _secondKeyToIdMap.getId(secondKey);
        if (secondKeyId == INVALID_ID) {
          return INVALID_ID;
        }
      } else {
        secondKeyId = NULL_ID;
      }
      long longKey = ((long) firstKeyId << 32) | (secondKeyId & 0xFFFFFFFFL);
      return _groupIdMap.get(longKey);
    }
  }

  @Override
  public int getNumGroups() {
    return _groupIdMap.size();
  }

  @Override
  public Iterator<GroupKey> getGroupKeyIterator(int numColumns) {
    return new Iterator<GroupKey>() {
      final ObjectIterator<Long2IntOpenHashMap.Entry> _entryIterator = _groupIdMap.long2IntEntrySet().fastIterator();

      @Override
      public boolean hasNext() {
        return _entryIterator.hasNext();
      }

      @Override
      public GroupKey next() {
        Long2IntMap.Entry entry = _entryIterator.next();
        long longKey = entry.getLongKey();
        Object[] row = new Object[numColumns];
        int firstKeyId = (int) (longKey >>> 32);
        int secondKeyId = (int) longKey;
        if (firstKeyId != NULL_ID) {
          row[0] = _firstKeyToIdMap.get(firstKeyId);
        }
        if (secondKeyId != NULL_ID) {
          row[1] = _secondKeyToIdMap.get(secondKeyId);
        }
        return new GroupKey(entry.getIntValue(), row);
      }
    };
  }
}

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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Iterator;


public class OneObjectKeyGroupIdGenerator implements GroupIdGenerator {
  private final Object2IntOpenHashMap<Object> _groupIdMap;
  private final int _numGroupsLimit;

  public OneObjectKeyGroupIdGenerator(int numGroupsLimit) {
    _groupIdMap = new Object2IntOpenHashMap<>();
    _groupIdMap.defaultReturnValue(INVALID_ID);
    _numGroupsLimit = numGroupsLimit;
  }

  @Override
  public int getGroupId(Object key) {
    int numGroups = _groupIdMap.size();
    if (numGroups < _numGroupsLimit) {
      return _groupIdMap.computeIntIfAbsent(key, k -> numGroups);
    } else {
      return _groupIdMap.getInt(key);
    }
  }

  @Override
  public int getNumGroups() {
    return _groupIdMap.size();
  }

  @Override
  public Iterator<GroupKey> getGroupKeyIterator(int numColumns) {
    return new Iterator<GroupKey>() {
      final ObjectIterator<Object2IntOpenHashMap.Entry<Object>> _entryIterator =
          _groupIdMap.object2IntEntrySet().fastIterator();

      @Override
      public boolean hasNext() {
        return _entryIterator.hasNext();
      }

      @Override
      public GroupKey next() {
        Object2IntMap.Entry<Object> entry = _entryIterator.next();
        Object[] row = new Object[numColumns];
        row[0] = entry.getKey();
        return new GroupKey(entry.getIntValue(), row);
      }
    };
  }
}

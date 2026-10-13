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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.ToIntFunction;


public class OneObjectKeyGroupIdGenerator implements GroupIdGenerator {
  private final Object2IntOpenHashMap<Object> _groupIdMap;
  private final int _numGroupsLimit;
  /// A function to generate the next group ID based on the current size of the map.
  /// We use this instead of a simple lambda to avoid capturing `this` and therefore allocate on each getGroupId call
  private final ToIntFunction<Object> _groupIdGenerator;

  public OneObjectKeyGroupIdGenerator(int numGroupsLimit, int initialCapacity) {
    _groupIdMap = new Object2IntOpenHashMap<>(initialCapacity);
    _groupIdMap.defaultReturnValue(INVALID_ID);
    _numGroupsLimit = numGroupsLimit;
    _groupIdGenerator = k -> _groupIdMap.size();
  }

  @Override
  public int getGroupId(Object key) {
    if (key != null && key.getClass().isArray()) {
      key = new ArrayKey(key);
    }
    if (_groupIdMap.size() < _numGroupsLimit) {
      return _groupIdMap.computeIfAbsent(key, _groupIdGenerator);
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
        Object key = entry.getKey();
        row[0] = key instanceof ArrayKey ? ((ArrayKey) key)._value : key;
        return new GroupKey(entry.getIntValue(), row);
      }
    };
  }

  /// Keeps array-valued keys equal across serialized spill rounds without changing their output representation.
  private static final class ArrayKey {
    private final Object _value;

    private ArrayKey(Object value) {
      _value = value;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof ArrayKey && Objects.deepEquals(_value, ((ArrayKey) other)._value);
    }

    @Override
    public int hashCode() {
      return Arrays.deepHashCode(new Object[]{_value});
    }
  }
}

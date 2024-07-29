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

import java.util.Iterator;


public interface GroupIdGenerator {
  int INVALID_ID = -1;
  int NULL_ID = -2;

  /**
   * Returns the group id for the given key. When a new key is encountered, it assigns a new group id to it before
   * reaching the groups limit, or returns {@link #INVALID_ID} when the limit is reached.
   * For single key column, the input is a single Object. For multi key columns, the input is an Object[] containing
   * the values for each key column.
   */
  int getGroupId(Object key);

  int getNumGroups();

  Iterator<GroupKey> getGroupKeyIterator(int numColumns);

  class GroupKey {
    public final int _groupId;
    // Row is pre-allocated for key and value columns, and is safe to be modified
    public final Object[] _row;

    public GroupKey(int groupId, Object[] row) {
      _groupId = groupId;
      _row = row;
    }
  }
}

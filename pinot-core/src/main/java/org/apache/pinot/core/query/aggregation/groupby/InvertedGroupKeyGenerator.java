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
package org.apache.pinot.core.query.aggregation.groupby;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.pinot.core.operator.blocks.ValueBlock;


public class InvertedGroupKeyGenerator implements GroupKeyGenerator {
  private final List<GroupKey> _groupKeyList;

  public InvertedGroupKeyGenerator(Object[] values) {
    _groupKeyList = new ArrayList<>(values.length);
    for (int i = 0; i < values.length; i++) {
      GroupKey groupKey = new GroupKey();
      groupKey._groupId = i;
      groupKey._keys = new Object[]{values[i]};
      _groupKeyList.add(groupKey);
    }
  }

  @Override
  public int getGlobalGroupKeyUpperBound() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[] groupKeys) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[][] groupKeys) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getCurrentGroupKeyUpperBound() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<GroupKey> getGroupKeys() {
    return _groupKeyList.iterator();
  }

  @Override
  public int getNumKeys() {
    return _groupKeyList.size();
  }
}

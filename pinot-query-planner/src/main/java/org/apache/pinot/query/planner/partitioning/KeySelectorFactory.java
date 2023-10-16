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
package org.apache.pinot.query.planner.partitioning;

import java.util.List;


public class KeySelectorFactory {
  private KeySelectorFactory() {
  }

  public static KeySelector<?> getKeySelector(List<Integer> keyIds) {
    int numKeys = keyIds.size();
    if (numKeys == 0) {
      return EmptyKeySelector.INSTANCE;
    } else if (numKeys == 1) {
      return new SingleColumnKeySelector(keyIds.get(0));
    } else {
      int[] ids = new int[numKeys];
      for (int i = 0; i < numKeys; i++) {
        ids[i] = keyIds.get(i);
      }
      return new MultiColumnKeySelector(ids);
    }
  }
}

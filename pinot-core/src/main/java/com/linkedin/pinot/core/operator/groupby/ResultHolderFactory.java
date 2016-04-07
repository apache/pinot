/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.groupby;

public class ResultHolderFactory {
  public static final int MAX_NUM_GROUP_KEYS_FOR_ARRAY_BASED = 1000000;

  /**
   * Creates and returns the appropriate implementation of ResultHolder,
   * based on the maximum number of results.
   *
   * For max number of results below a certain threshold {@value #MAX_NUM_GROUP_KEYS_FOR_ARRAY_BASED}
   * ArrayBasedResultHolder is returned, otherwise, MapBasedResultHolder is returned.
   *
   * @param maxNumResults
   * @return
   */
  public static ResultHolder getResultHolder(long maxNumResults, double defaultValue) {
    if (maxNumResults <= MAX_NUM_GROUP_KEYS_FOR_ARRAY_BASED) {
      return new ArrayBasedResultHolder((int) maxNumResults, defaultValue);
    } else {
      return new MapBasedResultHolder(defaultValue);
    }
  }
}

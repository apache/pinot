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

public interface ResultHolder {

  /**
   * Returns the handle to the internal result set storage.
   *
   * @return
   */
  Object getResultStore();

  /**
   * Return result for the given long group key.
   * @param groupKey
   * @return
   */
  double getResultForGroupKey(long groupKey);

  /**
   * Increase internal storage if needed to store the required number
   * of unique group keys.
   *
   * @param maxUniqueKeys
   */
  void ensureCapacity(int maxUniqueKeys);

  /**
   * Returns the value for the given group by key.
   * If the group key does not exist in the result holder, returns
   * the defaultValue it was initialized with.
   *
   * @param groupKey
   * @return
   */
  double getValueForKey(long groupKey);

  /**
   * Stores the given value for the given groupKey.
   *
   * @param groupKey
   * @param newValue
   */
  void putValueForKey(long groupKey, double newValue);
}

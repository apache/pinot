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

public interface ResultHolder<ResultType> {

  /**
   * Increase internal storage if needed to store the required number
   * of unique group keys.
   *
   * @param maxUniqueKeys
   */
  void ensureCapacity(int maxUniqueKeys);

  /**
   * Returns the double result. This version is for aggregation functions
   * without group by.
   * @return
   */
  double getDoubleResult();

  /**
   * Returns the result (double) for the given group by key.
   * If the group key does not exist in the result holder, returns
   * the defaultValue it was initialized with (default value of the aggregation
   * function it is holding the result for).
   *
   * @param groupKey
   * @return
   */
  double getDoubleResult(int groupKey);

  /**
   * Returns the result (ResultType) of aggregation. This version is
   * for aggregation without group by.
   * @return
   */
  ResultType getResult();

  /**
   * Returns the result (ResultType) for the given group key.
   * If the group key does not exist in the result holder, returns the
   * defaultValue it was initialized with (default value of the aggregation
   * function it is holding the result for).
   *
   * @param groupKey
   * @return
   */
  ResultType getResult(long groupKey);

  /**
   * Set the result value. This is for aggregation functions without group by.
   * @param newValue
   */
  void setValue(double newValue);

  /**
   * Set the result value. This is for aggregation functions without group by.
   * @param newValue
   */
  void setValue(ResultType newValue);

  /**
   * Stores the given value (of type double) for the given groupKey.
   *
   * @param groupKey
   * @param newValue
   */
  void setValueForKey(long groupKey, double newValue);

  /**
   * Store the given value (of type ResultType) for the given groupKey.
   * @param groupKey
   * @param newValue
   */
  void setValueForKey(long groupKey, ResultType newValue);
}

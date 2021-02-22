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

import java.util.Iterator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;


/**
 * This class holds the result of aggregation group by queries.
 * It provides an iterator over group-by keys, and provides a method
 * to get the aggregation result for the given group-by key.
 */
@SuppressWarnings("rawtypes")
public class AggregationGroupByResult {
  private final GroupKeyGenerator _groupKeyGenerator;
  private final AggregationFunction[] _aggregationFunctions;
  private final GroupByResultHolder[] _resultHolders;

  public AggregationGroupByResult(GroupKeyGenerator groupKeyGenerator, AggregationFunction[] aggregationFunctions,
      GroupByResultHolder[] resultHolders) {
    _groupKeyGenerator = groupKeyGenerator;
    _aggregationFunctions = aggregationFunctions;
    _resultHolders = resultHolders;
  }

  /**
   * Returns an iterator of {@link GroupKeyGenerator.GroupKey}.
   */
  public Iterator<GroupKeyGenerator.GroupKey> getGroupKeyIterator() {
    return _groupKeyGenerator.getGroupKeys();
  }

  /**
   * Returns an iterator of {@link GroupKeyGenerator.StringGroupKey}.
   */
  public Iterator<GroupKeyGenerator.StringGroupKey> getStringGroupKeyIterator() {
    return _groupKeyGenerator.getStringGroupKeys();
  }

  /**
   *
   * Given a group-by key and an index into the result holder array, returns
   * the corresponding aggregation result.
   *
   * Clients are expected to order the resultHolder array in the same order in which
   * the aggregation functions appear in the brokerRequest. This way they can simply
   * pass the index of aggregation function and get the result. This is purely a
   * runtime optimization, so that:
   * - Same groupKey can be re-used for multiple aggregation functions.
   * - Avoid any name based association (and lookup) between aggregation function
   *   and its result in this class.
   *
   * @param groupKey
   * @param index
   * @return
   */
  public Object getResultForKey(GroupKeyGenerator.StringGroupKey groupKey, int index) {
    return getResultForGroupId(index, groupKey._groupId);
  }

  public Object getResultForGroupId(int index, int groupId) {
    return _aggregationFunctions[index].extractGroupByResult(_resultHolders[index], groupId);
  }
}

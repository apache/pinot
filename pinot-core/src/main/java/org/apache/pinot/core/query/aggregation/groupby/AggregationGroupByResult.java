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

  public int getNumGroups() {
    return _groupKeyGenerator.getNumKeys();
  }

  /**
   * Returns an iterator of {@link GroupKeyGenerator.GroupKey}.
   */
  public Iterator<GroupKeyGenerator.GroupKey> getGroupKeyIterator() {
    return _groupKeyGenerator.getGroupKeys();
  }

  public Object getResultForGroupId(int index, int groupId) {
    return _aggregationFunctions[index].extractGroupByResult(_resultHolders[index], groupId);
  }
}

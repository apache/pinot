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
package org.apache.pinot.core.query.aggregation.function.funnel;

import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;


/**
 * Interface for segment aggregation result extraction strategy.
 *
 * <p>The implementation should be stateless, and can be shared among multiple segments in multiple threads.
 *
 * @param <A> Aggregation result accumulated across blocks within segment, kept by result holder.
 * @param <I> Intermediate result at segment level (extracted from aforementioned aggregation result).
 */
@ThreadSafe
interface ResultExtractionStrategy<A, I> {

  /**
   * Extracts the intermediate result from the aggregation result holder (aggregation only).
   */
  default I extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return extractIntermediateResult(aggregationResultHolder.getResult());
  }

  /**
   * Extracts the intermediate result from the group-by result holder for the given group key (aggregation group-by).
   */
  default I extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return extractIntermediateResult(groupByResultHolder.getResult(groupKey));
  }

  I extractIntermediateResult(A aggregationResult);
}

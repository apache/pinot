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

import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * Aggregation strategy for segments partitioned and sorted by the main correlation column.
 * For multi-key correlate-by, data must be sorted by the first (primary) column; secondary
 * keys are handled within each primary-key group by {@link SortedAggregationResult}.
 */
class SortedAggregationStrategy extends AggregationStrategy<SortedAggregationResult> {
  public SortedAggregationStrategy(List<ExpressionContext> stepExpressions,
      List<ExpressionContext> correlateByExpressions) {
    super(stepExpressions, correlateByExpressions);
  }

  @Override
  public SortedAggregationResult createAggregationResult(Dictionary[] dictionaries) {
    return new SortedAggregationResult(_numSteps, dictionaries.length);
  }

  @Override
  void add(SortedAggregationResult aggResult, int step, Dictionary[] dictionaries, int[] correlationDictIds) {
    if (correlationDictIds.length == 1) {
      aggResult.add(step, correlationDictIds[0]);
    } else {
      aggResult.addMultiKey(step, correlationDictIds);
    }
  }
}

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
package org.apache.pinot.core.operator.streaming;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.combine.merger.SelectionOnlyResultsBlockMerger;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Combine operator for selection queries with streaming response.
 */
@SuppressWarnings("rawtypes")
public class StreamingSelectionOnlyCombineOperator extends BaseStreamingCombineOperator<SelectionResultsBlock> {
  private static final String EXPLAIN_NAME = "STREAMING_COMBINE_SELECT";
  private final int _limit;

  public StreamingSelectionOnlyCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    super(new SelectionOnlyResultsBlockMerger(queryContext), operators, queryContext, executorService);
    _limit = queryContext.getLimit();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected boolean isChildOperatorSingleBlock() {
    return false;
  }

  @Override
  protected Object createQuerySatisfiedTracker() {
    return new AtomicLong();
  }

  @Override
  protected boolean isQuerySatisfied(SelectionResultsBlock resultsBlock, Object tracker) {
    return ((AtomicLong) tracker).addAndGet(resultsBlock.getNumRows()) >= _limit;
  }
}

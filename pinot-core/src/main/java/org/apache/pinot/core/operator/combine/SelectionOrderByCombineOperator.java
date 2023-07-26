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
package org.apache.pinot.core.operator.combine;

import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.combine.merger.SelectionOrderByResultsBlockMerger;
import org.apache.pinot.core.query.request.context.QueryContext;

/**
 * Combine operator for selection order-by queries.
 * <p>When the first order-by expression is an identifier (column), try to use
 * {@link org.apache.pinot.core.operator.combine.MinMaxValueBasedSelectionOrderByCombineOperator} first, which will
 * skip processing some segments based on the column min/max value. Otherwise fall back to the default combine
 * (process all segments).
 */
@SuppressWarnings("rawtypes")
public class SelectionOrderByCombineOperator extends BaseSingleBlockCombineOperator<SelectionResultsBlock> {
  private static final String EXPLAIN_NAME = "COMBINE_SELECT_ORDERBY";

  public SelectionOrderByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    super(new SelectionOrderByResultsBlockMerger(queryContext), operators, queryContext, executorService);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}

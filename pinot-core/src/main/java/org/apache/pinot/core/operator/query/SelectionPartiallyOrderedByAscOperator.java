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
package org.apache.pinot.core.operator.query;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * An operator for order-by queries ASC that are partially sorted over the sorting keys.
 * @see LinearSelectionOrderByOperator
 */
public class SelectionPartiallyOrderedByAscOperator extends LinearSelectionOrderByOperator {

  private static final String EXPLAIN_NAME = "SELECT_PARTIAL_ORDER_BY_ASC";

  private int _numDocsScanned = 0;

  public SelectionPartiallyOrderedByAscOperator(IndexSegment indexSegment, QueryContext queryContext,
      List<ExpressionContext> expressions, BaseProjectOperator<?> projectOperator, int numSortedExpressions) {
    super(indexSegment, queryContext, expressions, projectOperator, numSortedExpressions);
    Preconditions.checkArgument(queryContext.getOrderByExpressions().stream()
            .filter(expr -> expr.getExpression().getType() == ExpressionContext.Type.IDENTIFIER)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("The query is not order by identifiers"))
            .isAsc(),
        "%s can only be used when the first column in order by is ASC", EXPLAIN_NAME);
  }

  @Override
  protected List<Object[]> fetch(Supplier<ListBuilder> listBuilderSupplier) {
    int numExpressions = _expressions.size();
    BlockValSet[] blockValSets = new BlockValSet[numExpressions];
    ListBuilder listBuilder = listBuilderSupplier.get();
    ValueBlock valueBlock;
    while ((valueBlock = _projectOperator.nextBlock()) != null) {
      IntFunction<Object[]> rowFetcher = fetchBlock(valueBlock, blockValSets);
      int numDocsFetched = valueBlock.getNumDocs();
      _numDocsScanned += numDocsFetched;
      for (int i = 0; i < numDocsFetched; i++) {
        if (listBuilder.add(rowFetcher.apply(i))) {
          return listBuilder.build();
        }
      }
    }
    return listBuilder.build();
  }

  @Override
  public int getNumDocsScanned() {
    return _numDocsScanned;
  }

  @Override
  protected String getExplainName() {
    return EXPLAIN_NAME;
  }
}

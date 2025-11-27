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

import java.util.List;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.DocIdOrderedOperator;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * The operator used when selecting with order-by on a way that the segment layout can be used to prune results.
 *
 * This requires that the first order-by expression is on a column that is sorted in the segment and:
 * 1. The order-by is ASC
 * 2. The order-by is DESC and the input operators support descending iteration (e.g. full scan)
 *
 * @see LinearSelectionOrderByOperator
 */
public class SelectionPartiallyOrderedByLinearOperator extends LinearSelectionOrderByOperator {

  private static final String EXPLAIN_NAME = "SELECT_PARTIAL_ORDER_BY_LINEAR";

  private int _numDocsScanned = 0;

  public SelectionPartiallyOrderedByLinearOperator(IndexSegment indexSegment, QueryContext queryContext,
      List<ExpressionContext> expressions, BaseProjectOperator<?> projectOperator, int numSortedExpressions) {
    super(indexSegment, queryContext, expressions, projectOperator, numSortedExpressions);
    boolean firstColIsAsc = queryContext.getOrderByExpressions().stream()
        .filter(expr -> expr.getExpression().getType() == ExpressionContext.Type.IDENTIFIER)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("The query is not order by identifiers"))
        .isAsc();
    DocIdOrderedOperator.DocIdOrder docIdOrder = DocIdOrderedOperator.DocIdOrder.fromAsc(firstColIsAsc);
    if (!projectOperator.isCompatibleWith(docIdOrder)) {
      throw new IllegalStateException(EXPLAIN_NAME + " requires the input operator to be compatible with order: "
          + docIdOrder + ", but found: " + projectOperator.toExplainString());
    }
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
  protected String getUpperCaseExplainName() {
    return EXPLAIN_NAME;
  }
}

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

import com.google.common.base.CaseFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctExecutorFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * Operator for distinct queries on a single segment.
 */
public class DistinctOperator extends BaseOperator<DistinctResultsBlock> {
  private static final String EXPLAIN_NAME = "DISTINCT";
  private static final int UNLIMITED_ROWS = Integer.MAX_VALUE;

  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final BaseProjectOperator<?> _projectOperator;

  private int _numDocsScanned = 0;
  private final int _maxRowsInDistinct;
  private final int _numRowsWithoutChangeInDistinct;
  private int _numRowsWithoutNewDistinct = 0;
  private boolean _hitMaxRowsLimit = false;
  private boolean _hitNoChangeLimit = false;

  public DistinctOperator(IndexSegment indexSegment, QueryContext queryContext,
      BaseProjectOperator<?> projectOperator) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
    _projectOperator = projectOperator;
    Map<String, String> queryOptions = queryContext.getQueryOptions();
    if (queryOptions != null) {
      Integer maxRowsInDistinct = QueryOptionsUtils.getMaxRowsInDistinct(queryOptions);
      _maxRowsInDistinct = maxRowsInDistinct != null ? maxRowsInDistinct : UNLIMITED_ROWS;
      Integer numRowsWithoutChange = QueryOptionsUtils.getNumRowsWithoutChangeInDistinct(queryOptions);
      _numRowsWithoutChangeInDistinct =
          numRowsWithoutChange != null ? numRowsWithoutChange : UNLIMITED_ROWS;
    } else {
      _maxRowsInDistinct = UNLIMITED_ROWS;
      _numRowsWithoutChangeInDistinct = UNLIMITED_ROWS;
    }
  }

  @Override
  protected DistinctResultsBlock getNextBlock() {
    DistinctExecutor executor = DistinctExecutorFactory.getDistinctExecutor(_projectOperator, _queryContext);
    executor.setMaxRowsToProcess(_maxRowsInDistinct);
    ValueBlock valueBlock;
    boolean enforceRowLimit = _maxRowsInDistinct != UNLIMITED_ROWS;
    boolean enforceNoChangeLimit = _numRowsWithoutChangeInDistinct != UNLIMITED_ROWS;
    while ((valueBlock = _projectOperator.nextBlock()) != null) {
      if (enforceRowLimit && executor.getRemainingRowsToProcess() <= 0) {
        _hitMaxRowsLimit = true;
        break;
      }
      int rowsRemainingBefore = executor.getRemainingRowsToProcess();
      int distinctCountBeforeBlock = enforceNoChangeLimit ? executor.getNumDistinctRowsCollected() : -1;
      boolean satisfied = executor.process(valueBlock);
      int rowsRemainingAfter = executor.getRemainingRowsToProcess();
      int docsProcessedForLimit;
      if (enforceRowLimit && rowsRemainingBefore != UNLIMITED_ROWS && rowsRemainingAfter != UNLIMITED_ROWS) {
        docsProcessedForLimit = Math.max(0, rowsRemainingBefore - rowsRemainingAfter);
      } else {
        docsProcessedForLimit = valueBlock.getNumDocs();
      }
      _numDocsScanned += docsProcessedForLimit;
      if (enforceRowLimit && _numDocsScanned >= _maxRowsInDistinct) {
        _hitMaxRowsLimit = true;
      }
      if (enforceNoChangeLimit) {
        int distinctCountAfterBlock = executor.getNumDistinctRowsCollected();
        if (distinctCountAfterBlock > distinctCountBeforeBlock) {
          _numRowsWithoutNewDistinct = 0;
        } else {
          _numRowsWithoutNewDistinct += docsProcessedForLimit;
          if (_numRowsWithoutNewDistinct >= _numRowsWithoutChangeInDistinct) {
            _hitNoChangeLimit = true;
          }
        }
      }
      if (_hitMaxRowsLimit || _hitNoChangeLimit || satisfied) {
        break;
      }
    }
    DistinctResultsBlock resultsBlock = new DistinctResultsBlock(executor.getResult(), _queryContext);
    if (_hitMaxRowsLimit) {
      resultsBlock.setEarlyTerminationReason(BaseResultsBlock.EarlyTerminationReason.DISTINCT_MAX_ROWS);
    } else if (_hitNoChangeLimit) {
      resultsBlock.setEarlyTerminationReason(BaseResultsBlock.EarlyTerminationReason.DISTINCT_NO_NEW_VALUES);
    }
    return resultsBlock;
  }

  @Override
  public List<BaseProjectOperator<?>> getChildOperators() {
    return Collections.singletonList(_projectOperator);
  }

  @Override
  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _projectOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _projectOperator.getNumColumnsProjected();
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        numTotalDocs);
  }

  @Override
  public String toExplainString() {
    List<ExpressionContext> expressions = _queryContext.getSelectExpressions();
    int numExpressions = expressions.size();
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(keyColumns:");
    stringBuilder.append(expressions.get(0).toString());
    for (int i = 1; i < numExpressions; i++) {
      stringBuilder.append(", ").append(expressions.get(i).toString());
    }
    return stringBuilder.append(')').toString();
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    List<ExpressionContext> selectExpressions = _queryContext.getSelectExpressions();
    if (selectExpressions.isEmpty()) {
      return;
    }
    List<String> expressions = selectExpressions.stream()
        .map(ExpressionContext::toString)
        .collect(Collectors.toList());
    attributeBuilder.putStringList("keyColumns", expressions);
  }
}

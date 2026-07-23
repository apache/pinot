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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.query.aggregation.AggregationExecutor;
import org.apache.pinot.core.query.aggregation.DefaultAggregationExecutor;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils.AggregationInfo;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.executor.StarTreeAggregationExecutor;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.query.QueryScanCostContext;


/**
 * The <code>AggregationOperator</code> class implements keyless aggregation query on a single segment in V1/SSQE.
 */
@SuppressWarnings("rawtypes")
public class AggregationOperator extends BaseOperator<AggregationResultsBlock> {
  private static final String EXPLAIN_NAME = "AGGREGATE";

  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final BaseProjectOperator<?> _projectOperator;
  private final boolean _useStarTree;
  private final long _numTotalDocs;

  private int _numDocsScanned = 0;

  // Marks the functions that can be resolved from the column dictionary/metadata without scanning the segment. The
  // aligned dataSources array holds the argument data source of each such function (a null entry is only valid for
  // COUNT). Resolution is deferred to execution time (see getNextBlock) to keep query planning cheap. Both are null
  // when no function is metadata-resolvable, in which case every function is computed by scanning.
  @Nullable
  private final boolean[] _metadataResolvable;
  @Nullable
  private final DataSource[] _dataSources;

  public AggregationOperator(QueryContext queryContext, AggregationInfo aggregationInfo, long numTotalDocs) {
    this(queryContext, aggregationInfo, numTotalDocs, null, null);
  }

  /**
   * Constructs an aggregation operator that optionally resolves some functions from the column dictionary/metadata
   * instead of scanning the segment. For each function flagged in {@code metadataResolvable}, the result is resolved
   * from the aligned {@code dataSources} entry (via {@link AggregationFunctionUtils#getAggregationResult}) at execution
   * time and the function is skipped during the scan; all other functions are computed by scanning the segment.
   * Passing {@code null} for both means every function is computed by scanning.
   *
   * @param metadataResolvable per-function flags (aligned by function index) marking the functions to resolve from
   *     dictionary/metadata, or {@code null} if none are resolvable
   * @param dataSources per-function argument data sources aligned by function index (a {@code null} entry is only
   *     valid for {@code COUNT}), or {@code null} if none are resolvable
   */
  public AggregationOperator(QueryContext queryContext, AggregationInfo aggregationInfo, long numTotalDocs,
      @Nullable boolean[] metadataResolvable, @Nullable DataSource[] dataSources) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    _projectOperator = aggregationInfo.getProjectOperator();
    _useStarTree = aggregationInfo.isUseStarTree();
    _numTotalDocs = numTotalDocs;
    _metadataResolvable = metadataResolvable;
    _dataSources = dataSources;
  }

  @Override
  protected AggregationResultsBlock getNextBlock() {
    // Perform aggregation on all the transform blocks
    AggregationExecutor aggregationExecutor;
    if (_useStarTree) {
      // StarTreeAggregationExecutor doesn't support pre-aggregated results.
      aggregationExecutor = new StarTreeAggregationExecutor(_aggregationFunctions);
    } else {
      aggregationExecutor = new DefaultAggregationExecutor(_aggregationFunctions, resolveMetadataBasedResults());
    }
    ValueBlock valueBlock;
    while ((valueBlock = _projectOperator.nextBlock()) != null) {
      _numDocsScanned += valueBlock.getNumDocs();
      QueryScanCostContext scanCost = getScanCostContext();
      if (scanCost != null) {
        scanCost.addDocsScanned(valueBlock.getNumDocs());
        scanCost.addEntriesScannedPostFilter(
            (long) valueBlock.getNumDocs() * _projectOperator.getNumColumnsProjected());
      }
      aggregationExecutor.aggregate(valueBlock);
    }

    // Build intermediate result block based on aggregation result from the executor
    return new AggregationResultsBlock(_aggregationFunctions, aggregationExecutor.getResult(), _queryContext);
  }

  /**
   * Returns {@code null} when no function is metadata-resolvable, in which case all functions are computed by scanning.
   * Each returned non-null entry is consumed by {@link DefaultAggregationExecutor}, which skips the scan for that
   * function and emits the resolved value directly.
   */
  @Nullable
  private Object[] resolveMetadataBasedResults() {
    if (_metadataResolvable == null) {
      return null;
    }

    Objects.requireNonNull(_dataSources);
    Object[] preAggregatedResults = new Object[_aggregationFunctions.length];
    for (int i = 0; i < _aggregationFunctions.length; i++) {
      if (_metadataResolvable[i]) {
        preAggregatedResults[i] = AggregationFunctionUtils.getAggregationResult(_aggregationFunctions[i],
            _dataSources[i], (int) _numTotalDocs, EXPLAIN_NAME);
      }
    }
    return preAggregatedResults;
  }

  @Override
  public List<BaseProjectOperator<?>> getChildOperators() {
    return List.of(_projectOperator);
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _projectOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _projectOperator.getNumColumnsProjected();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        _numTotalDocs);
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(aggregations:");
    if (_aggregationFunctions.length > 0) {
      stringBuilder.append(_aggregationFunctions[0].toExplainString());
      for (int i = 1; i < _aggregationFunctions.length; i++) {
        stringBuilder.append(", ").append(_aggregationFunctions[i].toExplainString());
      }
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
    if (_aggregationFunctions.length == 0) {
      return;
    }
    List<String> aggregations = Arrays.stream(_aggregationFunctions)
        .map(AggregationFunction::toExplainString)
        .collect(Collectors.toList());
    attributeBuilder.putStringList("aggregations", aggregations);
  }
}

/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.aggregation.groupby;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.common.DataFetcher;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.operator.aggregation.ResultHolderFactory;
import com.linkedin.pinot.core.operator.aggregation.SingleValueBlockCache;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import java.util.List;


/**
 * This class implements group by aggregation.
 * It is optimized for performance, and uses the best possible algorithm/datastructure
 * for a given query based on the following parameters:
 * - Maximum number of group keys possible.
 * - Single/Multi valued columns.
 */
public class DefaultGroupByExecutor implements GroupByExecutor {
  private final SingleValueBlockCache _singleValueBlockCache;

  private final GroupKeyGenerator _groupKeyGenerator;
  private final int _numAggrFunc;
  private final AggregationFunctionContext[] _aggrFuncContextArray;
  private final GroupByResultHolder[] _resultHolderArray;

  private int[] _docIdToSVGroupKey;
  private int[][] _docIdToMVGroupKey;

  private boolean _hasMultiValuedColumns = false;
  private boolean _inited = false;
  private boolean _finished = false;

  /**
   * Constructor for the class.
   *
   * @param indexSegment
   * @param aggregationInfoList
   * @param groupBy
   * @param maxNumGroupKeys
   */
  public DefaultGroupByExecutor(IndexSegment indexSegment, List<AggregationInfo> aggregationInfoList, GroupBy groupBy,
      int maxNumGroupKeys) {
    Preconditions.checkNotNull(indexSegment);
    Preconditions.checkNotNull(aggregationInfoList);
    Preconditions.checkArgument(aggregationInfoList.size() > 0);
    Preconditions.checkNotNull(groupBy);
    Preconditions.checkArgument(maxNumGroupKeys > 0);

    DataFetcher dataFetcher = new DataFetcher(indexSegment);
    _singleValueBlockCache = new SingleValueBlockCache(dataFetcher);
    List<String> groupByColumnList = groupBy.getColumns();
    String[] groupByColumns = groupByColumnList.toArray(new String[groupByColumnList.size()]);
    _groupKeyGenerator = new DefaultGroupKeyGenerator(dataFetcher, groupByColumns, maxNumGroupKeys);

    _numAggrFunc = aggregationInfoList.size();
    _aggrFuncContextArray = new AggregationFunctionContext[_numAggrFunc];
    _resultHolderArray = new GroupByResultHolder[_numAggrFunc];
    for (int i = 0; i < _numAggrFunc; i++) {
      AggregationInfo aggregationInfo = aggregationInfoList.get(i);
      String[] columns = aggregationInfo.getAggregationParams().get("column").trim().split(",");
      AggregationFunctionContext aggregationFunctionContext =
          new AggregationFunctionContext(aggregationInfo.getAggregationType(), columns);
      _aggrFuncContextArray[i] = aggregationFunctionContext;
      AggregationFunction aggregationFunction = aggregationFunctionContext.getAggregationFunction();
      _resultHolderArray[i] = ResultHolderFactory.getGroupByResultHolder(aggregationFunction, maxNumGroupKeys);
    }

    _hasMultiValuedColumns = hasMultiValueGroupByColumns(indexSegment, groupByColumns);
  }

  /**
   * {@inheritDoc}
   *
   * Initializes the following:
   * - List of AggregationFunctionContexts for the given query.
   * - Group by key generator.
   * - Various re-usable arrays that store dictionaries/values/results.
   */
  @Override
  public void init() {
    // Returned if already initialized.
    if (_inited) {
      return;
    }

    if (_hasMultiValuedColumns) {
      _docIdToMVGroupKey = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    } else {
      _docIdToSVGroupKey = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    _inited = true;
  }

  /**
   * Process the provided set of docId's to perform the requested aggregation-group-by-operation.
   *
   * @param docIdSet
   * @param startIndex
   * @param length
   */
  @Override
  public void process(int[] docIdSet, int startIndex, int length) {
    Preconditions
        .checkState(_inited, "Method 'process' cannot be called before 'init' for class " + getClass().getName());

    _singleValueBlockCache.initNewBlock(docIdSet, startIndex, length);

    generateGroupKeysForDocIdSet(docIdSet, startIndex, length);
    int numGroupKeys = _groupKeyGenerator.getNumGroupKeys();

    for (int i = 0; i < _numAggrFunc; i++) {
      _resultHolderArray[i].ensureCapacity(numGroupKeys);
      aggregateColumn(_aggrFuncContextArray[i], _resultHolderArray[i], length);
    }
  }

  /**
   * Helper method to perform aggregation for a given column.
   *
   * @param aggrFuncContext
   * @param resultHolder
   * @param length
   */
  private void aggregateColumn(AggregationFunctionContext aggrFuncContext, GroupByResultHolder resultHolder,
      int length) {
    AggregationFunction aggregationFunction = aggrFuncContext.getAggregationFunction();
    String[] aggrColumns = aggrFuncContext.getAggregationColumns();
    String aggrFuncName = aggregationFunction.getName();

    Preconditions.checkState(aggrColumns.length == 1);
    String aggrColumn = aggrColumns[0];

    switch (aggrFuncName) {
      case AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION:
        if (_hasMultiValuedColumns) {
          aggregationFunction.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder);
        } else {
          aggregationFunction.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder);
        }
        break;

      case AggregationFunctionFactory.DISTINCTCOUNT_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.DISTINCTCOUNTHLL_AGGREGATION_FUNCTION:
        double[] hashCodeArray = _singleValueBlockCache.getHashCodeArrayForColumn(aggrColumn);
        if (_hasMultiValuedColumns) {
          aggregationFunction.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder, hashCodeArray);
        } else {
          aggregationFunction.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder, hashCodeArray);
        }
        break;

      default:
        double[] valueArray = _singleValueBlockCache.getDoubleValueArrayForColumn(aggrColumn);
        if (_hasMultiValuedColumns) {
          aggregationFunction.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder, valueArray);
        } else {
          aggregationFunction.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder, valueArray);
        }
        break;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void finish() {
    Preconditions
        .checkState(_inited, "Method 'finish' cannot be called before 'init' for class " + getClass().getName());

    _finished = true;
  }

  /**
   * Return the final result of the aggregation-group-by operation.
   * This method should be called after all docIdSets have been 'processed'.
   *
   * @return
   */
  @Override
  public AggregationGroupByResult getResult() {
    Preconditions
        .checkState(_finished, "Method 'getResult' cannot be called before 'finish' for class " + getClass().getName());

    AggregationFunction.ResultDataType[] resultDataTypeArray = new AggregationFunction.ResultDataType[_numAggrFunc];

    for (int i = 0; i < _numAggrFunc; i++) {
      AggregationFunction aggregationFunction = _aggrFuncContextArray[i].getAggregationFunction();
      resultDataTypeArray[i] = aggregationFunction.getResultDataType();
    }
    return new AggregationGroupByResult(_groupKeyGenerator, _resultHolderArray, resultDataTypeArray);
  }

  /**
   * Returns true if any of the group-by columns are multi-valued, false otherwise.
   *
   * @param indexSegment
   * @param groupByColumns
   * @return
   */
  private static boolean hasMultiValueGroupByColumns(IndexSegment indexSegment, String[] groupByColumns) {
    for (String groupByColumn : groupByColumns) {
      DataSource dataSource = indexSegment.getDataSource(groupByColumn);
      DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();

      if (!dataSourceMetadata.isSingleValue()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Generate group keys for the given docIdSet. For single valued columns, each docId has one group key,
   * but for multi-valued columns, each docId could have more than one group key.
   *
   * For SV keys: _docIdToSVGroupKey mapping is updated.
   * For MV keys: _docIdToMVGroupKey mapping is updated.
   *
   * @param docIdSet
   * @param startIndex
   * @param length
   */
  private void generateGroupKeysForDocIdSet(int[] docIdSet, int startIndex, int length) {
    if (_hasMultiValuedColumns) {
      _groupKeyGenerator.generateKeysForDocIdSet(docIdSet, startIndex, length, _docIdToMVGroupKey);
    } else {
      _groupKeyGenerator.generateKeysForDocIdSet(docIdSet, startIndex, length, _docIdToSVGroupKey);
    }
  }
}

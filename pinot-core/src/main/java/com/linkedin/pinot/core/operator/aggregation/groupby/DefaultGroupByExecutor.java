/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.DataFetcher;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.operator.aggregation.ResultHolderFactory;
import com.linkedin.pinot.core.operator.aggregation.DataBlockCache;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import java.util.List;
import java.util.Map;


/**
 * This class implements group by aggregation.
 * It is optimized for performance, and uses the best possible algorithm/datastructure
 * for a given query based on the following parameters:
 * - Maximum number of group keys possible.
 * - Single/Multi valued columns.
 */
public class DefaultGroupByExecutor implements GroupByExecutor {

  private static final double GROUP_BY_TRIM_FACTOR = 0.9;
  private final DataBlockCache _singleValueBlockCache;
  private final GroupKeyGenerator _groupKeyGenerator;
  private final int _numAggrFunc;
  private final AggregationFunctionContext[] _aggrFuncContextArray;
  private final GroupByResultHolder[] _resultHolderArray;
  private final int _trimSize;

  private int[] _docIdToSVGroupKey;
  private int[][] _docIdToMVGroupKey;

  private boolean _hasMultiValuedColumns = false;
  private boolean _inited = false;
  private boolean _finished = false;
  private DataFetcher _dataFetcher;

  /**
   * Constructor for the class.
   *
   * @param aggregationInfoList Aggregation info from broker request
   * @param groupBy Group by from broker request
   * @param numGroupsLimit Limit on number of aggregation groups returned in the result
   */
  public DefaultGroupByExecutor(MProjectionOperator projectionOperator, List<AggregationInfo> aggregationInfoList, GroupBy groupBy,
      int numGroupsLimit) {

    Preconditions.checkNotNull(aggregationInfoList);
    Preconditions.checkArgument(aggregationInfoList.size() > 0);
    Preconditions.checkNotNull(groupBy);

    Map<String, BaseOperator> dataSourceMap = projectionOperator.getDataSourceMap();
    _dataFetcher = new DataFetcher(dataSourceMap);

    _singleValueBlockCache = new DataBlockCache(_dataFetcher);

    List<String> groupByColumnList = groupBy.getColumns();
    String[] groupByColumns = groupByColumnList.toArray(new String[groupByColumnList.size()]);


    _groupKeyGenerator = new DefaultGroupKeyGenerator(_dataFetcher, groupByColumns);

    // Maximum number of results is the minimum of possible keys, and limit on number of groups
    int maxNumResults = Math.min(_groupKeyGenerator.getGlobalGroupKeyUpperBound(), numGroupsLimit);

    // When results are trimmed, drop bottom 10% of groups.
    _trimSize = (int) (GROUP_BY_TRIM_FACTOR * maxNumResults);

    _hasMultiValuedColumns = _groupKeyGenerator.hasMultiValueGroupByColumn();

    _numAggrFunc = aggregationInfoList.size();
    _aggrFuncContextArray = new AggregationFunctionContext[_numAggrFunc];
    _resultHolderArray = new GroupByResultHolder[_numAggrFunc];

    for (int i = 0; i < _numAggrFunc; i++) {
      AggregationInfo aggregationInfo = aggregationInfoList.get(i);
      String[] columns = aggregationInfo.getAggregationParams().get("column").trim().split(",");

      _aggrFuncContextArray[i] = new AggregationFunctionContext(
          aggregationInfo.getAggregationType(), columns);
      _resultHolderArray[i] = ResultHolderFactory.getGroupByResultHolder(
          _aggrFuncContextArray[i].getAggregationFunction(), maxNumResults);
    }
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
    int capacityNeeded = _groupKeyGenerator.getCurrentGroupKeyUpperBound();

    for (int i = 0; i < _numAggrFunc; i++) {
      _resultHolderArray[i].ensureCapacity(capacityNeeded);
      aggregateColumn(_aggrFuncContextArray[i], _resultHolderArray[i], length);

      // Result holder limits the max number of group keys (default 100k), if the number of groups
      // exceeds beyond that limit, groups with lower values (as per sort order) are trimmed.
      // Once result holder trims those groups, the group key generator needs to purge them.

      int[] trimmedKeys = _resultHolderArray[i].trimResults(_trimSize);
      _groupKeyGenerator.purgeKeys(trimmedKeys);
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
    boolean isAggrColumnsingleValueField = true;
    BlockMetadata blockMetadata = _dataFetcher.getBlockMetadataFor(aggrColumn);
    if (blockMetadata != null) {
      isAggrColumnsingleValueField = blockMetadata.isSingleValue();
    }
    switch (aggrFuncName) {
      case AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION:
        if (_hasMultiValuedColumns) {
          aggregationFunction.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder);
        } else {
          aggregationFunction.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder);
        }
        break;

      case AggregationFunctionFactory.COUNT_MV_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.DISTINCTCOUNT_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.DISTINCTCOUNTHLL_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.DISTINCTCOUNT_MV_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.DISTINCTCOUNTHLL_MV_AGGREGATION_FUNCTION:
        Object hashCodeArray;
        if (isAggrColumnsingleValueField) {
          hashCodeArray = _singleValueBlockCache.getHashCodeArrayForColumn(aggrColumn);
        } else {
          hashCodeArray = _singleValueBlockCache.getHashCodesArrayForColumn(aggrColumn);
        }
        if (_hasMultiValuedColumns) {
          aggregationFunction.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder, hashCodeArray);
        } else {
          aggregationFunction.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder, hashCodeArray);
        }
        break;

      case AggregationFunctionFactory.FASTHLL_AGGREGATION_FUNCTION:
        Object stringArray;
        if (isAggrColumnsingleValueField) {
          stringArray = _singleValueBlockCache.getStringValueArrayForColumn(aggrColumn);
        } else {
          stringArray = _singleValueBlockCache.getStringValuesArrayForColumn(aggrColumn);
        }
        if (_hasMultiValuedColumns) {
          aggregationFunction.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder, (Object) stringArray);
        } else {
          aggregationFunction.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder, (Object) stringArray);
        }
        break;

      default:
        Object valueArray;
        if (isAggrColumnsingleValueField) {
          valueArray = _singleValueBlockCache.getDoubleValueArrayForColumn(aggrColumn);
        } else {
          valueArray = _singleValueBlockCache.getDoubleValuesArrayForColumn(aggrColumn);
        }

        if (_hasMultiValuedColumns) {
          aggregationFunction.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder, (Object) valueArray);
        } else {
          aggregationFunction.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder, (Object) valueArray);
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

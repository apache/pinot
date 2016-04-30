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
package com.linkedin.pinot.core.operator.groupby;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.utils.primitive.MutableLongValue;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.quantile.digest.QuantileDigest;
import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * This class implements group by aggregation.
 * It is optimized for performance, and uses the best possible algorithm/datastructure
 * for a given query based on the following parameters:
 * - Maximum number of group keys possible.
 * - Single/Multi valued columns.
 */
public class DefaultGroupByExecutor implements GroupByExecutor {
  private final IndexSegment _indexSegment;
  private final List<AggregationInfo> _aggregationsInfoList;
  private final GroupBy _groupBy;
  private final int _maxNumGroupKeys;

  private ArrayList<AggregationFunctionContext> _aggrFuncContextList;
  private GroupKeyGenerator _groupKeyGenerator;

  private String[] _groupByColumns;
  private int[] _docIdToSVGroupKey;
  private int[][] _docIdToMVGroupKey;

  private Set<String> _columnsLoaded;
  private Map<String, int[]> _columnToDictArrayMap;
  private Map<String, double[]> _columnToValueArrayMap;
  private double[] _hashCodeArray;

  private GroupByResultHolder[] _resultHolderArray;

  private boolean _hasMultiValuedColumns = false;
  private boolean _inited = false;
  private boolean _finished = false;

  /**
   * Constructor for the class.
   *
   * @param indexSegment
   * @param aggregationsInfoList
   * @param groupBy
   * @param maxNumGroupKeys
   */
  public DefaultGroupByExecutor(IndexSegment indexSegment, List<AggregationInfo> aggregationsInfoList, GroupBy groupBy,
      int maxNumGroupKeys) {
    Preconditions.checkNotNull(indexSegment);
    Preconditions.checkArgument((aggregationsInfoList != null) && (aggregationsInfoList.size() > 0));
    Preconditions.checkNotNull(groupBy);
    Preconditions.checkArgument(maxNumGroupKeys > 0);

    _indexSegment = indexSegment;
    _aggregationsInfoList = aggregationsInfoList;
    _groupBy = groupBy;
    _maxNumGroupKeys = maxNumGroupKeys;
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

    List<String> groupByColumns = _groupBy.getColumns();
    _groupByColumns = groupByColumns.toArray(new String[groupByColumns.size()]);

    _aggrFuncContextList = new ArrayList<AggregationFunctionContext>(_aggregationsInfoList.size());
    _columnsLoaded = new HashSet<>();
    _columnToDictArrayMap = new HashMap<String, int[]>();
    _columnToValueArrayMap = new HashMap<String, double[]>();

    for (AggregationInfo aggregationInfo : _aggregationsInfoList) {
      String[] columns = aggregationInfo.getAggregationParams().get("column").trim().split(",");

      for (String column : columns) {
        if (!column.equals("*") && !_columnToDictArrayMap.containsKey(column)) {
          _columnToDictArrayMap.put(column, new int[DocIdSetPlanNode.MAX_DOC_PER_CALL]);
          _columnToValueArrayMap.put(column, new double[DocIdSetPlanNode.MAX_DOC_PER_CALL]);
        }
      }

      AggregationFunctionContext aggregationFunctionContext =
          new AggregationFunctionContext(_indexSegment, aggregationInfo.getAggregationType(), columns);
      _aggrFuncContextList.add(aggregationFunctionContext);
    }

    _hasMultiValuedColumns = hasMultiValueGroupByColumns(_indexSegment, _groupByColumns);
    _groupKeyGenerator = new DefaultGroupKeyGenerator(_indexSegment, _groupByColumns, _maxNumGroupKeys);

    _resultHolderArray = new GroupByResultHolder[_aggrFuncContextList.size()];
    for (int i = 0; i < _aggrFuncContextList.size(); i++) {
      AggregationFunctionContext aggregationFunctionContext = _aggrFuncContextList.get(i);
      AggregationFunction aggregationFunction = aggregationFunctionContext.getAggregationFunction();
      _resultHolderArray[i] = ResultHolderFactory.getGroupByResultHolder(aggregationFunction, _maxNumGroupKeys);
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
    Preconditions.checkState(_inited, "Method 'process' cannot be called before init.");

    generateGroupKeysForDocIdSet(docIdSet, startIndex, length);
    int numGroupKeys = _groupKeyGenerator.getNumGroupKeys();

    fetchColumnDictIds(docIdSet, startIndex, length);
    fetchColumnValues(startIndex, length);

    for (int i = 0; i < _aggrFuncContextList.size(); i++) {
      AggregationFunctionContext aggrFuncContext = _aggrFuncContextList.get(i);
      _resultHolderArray[i].ensureCapacity(numGroupKeys);
      aggregateColumn(aggrFuncContext, _resultHolderArray[i], length);
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
    AggregationFunction function = aggrFuncContext.getAggregationFunction();
    String aggrFuncName = aggrFuncContext.getFunctionName();
    String[] aggrColumns = aggrFuncContext.getAggregationColumns();

    Preconditions.checkState(aggrColumns.length == 1);
    String aggrColumn = aggrColumns[0];

    switch (aggrFuncName) {
      case AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION:
        if (_hasMultiValuedColumns) {
          function.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder);
        } else {
          function.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder);
        }
        return;

      case AggregationFunctionFactory.DISTINCTCOUNT_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.DISTINCTCOUNTHLL_AGGREGATION_FUNCTION:
        fetchColumnValueHashCodes(aggrColumn, aggrFuncContext.getDictionary(0), length);
        if (_hasMultiValuedColumns) {
          function.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder, _hashCodeArray);
        } else {
          function.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder, _hashCodeArray);
        }
        return;

      default:
        double[] valueArray = _columnToValueArrayMap.get(aggrColumn);

        if (_hasMultiValuedColumns) {
          function.aggregateGroupByMV(length, _docIdToMVGroupKey, resultHolder, valueArray);
        } else {
          function.aggregateGroupBySV(length, _docIdToSVGroupKey, resultHolder, valueArray);
        }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void finish() {
    _finished = true;
  }

  /**
   * Return the final result of the aggregation-group-by operation.
   * This method should be called after all docIdSets have been 'processed'.
   *
   * @return
   */
  @Override
  public List<Map<String, Serializable>> getResult() {
    Preconditions.checkState(_finished, "GetResult cannot be called before finish.");
    List<Map<String, Serializable>> result = new ArrayList<Map<String, Serializable>>(_aggrFuncContextList.size());

    for (int i = 0; i < _aggrFuncContextList.size(); i++) {
      result.add(new HashMap<String, Serializable>());
    }

    Iterator<Pair<Integer, String>> groupKeys = _groupKeyGenerator.getUniqueGroupKeys();
    while (groupKeys.hasNext()) {
      Pair<Integer, String> idKeyPair = groupKeys.next();
      String stringGroupKey = idKeyPair.getSecond();

      for (int i = 0; i < _aggrFuncContextList.size(); i++) {
        AggregationFunction aggregationFunction = _aggrFuncContextList.get(i).getAggregationFunction();
        AggregationFunction.ResultDataType resultDataType = aggregationFunction.getResultDataType();

        Serializable resultForGroupKey = getResultForKey(idKeyPair.getFirst(), _resultHolderArray[i], resultDataType);
        result.get(i).put(stringGroupKey, resultForGroupKey);
      }
    }
    return result;
  }

  /**
   * Returns result for the given key.
   *
   * @param groupKey
   * @param resultDataType
   * @return
   */
  private Serializable getResultForKey(int groupKey, GroupByResultHolder resultHolder,
      AggregationFunction.ResultDataType resultDataType) {

    switch (resultDataType) {
      case LONG:
        return new MutableLongValue((long) resultHolder.getDoubleResult(groupKey));

      case DOUBLE:
        return resultHolder.getDoubleResult(groupKey);

      case AVERAGE_PAIR:
        Pair<Double, Long> doubleLongPair = (Pair<Double, Long>) resultHolder.getResult(groupKey);
        return new AvgAggregationFunction.AvgPair(doubleLongPair.getFirst(), doubleLongPair.getSecond());

      case MINMAXRANGE_PAIR:
        Pair<Double, Double> doubleDoublePair = (Pair<Double, Double>) resultHolder.getResult(groupKey);
        return new MinMaxRangeAggregationFunction.MinMaxRangePair(doubleDoublePair.getFirst(),
            doubleDoublePair.getSecond());

      case DISTINCTCOUNT_SET:
        return (IntOpenHashSet) resultHolder.getResult(groupKey);

      case DISTINCTCOUNTHLL_HYPERLOGLOG:
        return (HyperLogLog) resultHolder.getResult(groupKey);

      case PERCENTILE_LIST:
        return (DoubleArrayList) resultHolder.getResult(groupKey);

      case PERCENTILEEST_QUANTILEDIGEST:
        return (QuantileDigest) resultHolder.getResult(groupKey);

      default:
        throw new RuntimeException(
            "Unsupported result data type in class " + getClass().getName());
    }
  }

  /**
   * Fetch dictId's for the given docIdSet for all aggregation columns except count.
   *
   * @param docIdSet
   * @param startIndex
   * @param length
   */
  private void fetchColumnDictIds(int[] docIdSet, int startIndex, int length) {
    _columnsLoaded.clear();
    for (AggregationFunctionContext aggrFuncContext : _aggrFuncContextList) {
      String aggrFuncName = aggrFuncContext.getFunctionName();
      if (!aggrFuncName.equals(AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION)) {
        String[] aggrColumns = aggrFuncContext.getAggregationColumns();
        for (int i = 0; i < aggrColumns.length; i++) {
          String aggrColumn = aggrColumns[i];

          if (!_columnsLoaded.contains(aggrColumn)) {
            int[] dictIdArray = _columnToDictArrayMap.get(aggrColumn);
            BlockValSet blockValSet = aggrFuncContext.getBlockValSet(i);
            blockValSet.readIntValues(docIdSet, startIndex, length, dictIdArray, startIndex);
            _columnsLoaded.add(aggrColumn);
          }
        }
      }
    }
  }

  /**
   * Fetch values for all aggregation columns except count and distinctcount according to the dictId's that fetched in
   * method fetchColumnDictIds.
   *
   * @param startIndex
   * @param length
   */
  private void fetchColumnValues(int startIndex, int length) {
    _columnsLoaded.clear();
    for (AggregationFunctionContext aggrFuncContext : _aggrFuncContextList) {
      String aggrFuncName = aggrFuncContext.getFunctionName();
      if (!aggrFuncName.equals(AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION)
          && !aggrFuncName.equals(AggregationFunctionFactory.DISTINCTCOUNT_AGGREGATION_FUNCTION)
          && !aggrFuncName.equals(AggregationFunctionFactory.DISTINCTCOUNTHLL_AGGREGATION_FUNCTION)) {
        String[] aggrColumns = aggrFuncContext.getAggregationColumns();
        for (int i = 0; i < aggrColumns.length; i++) {
          String aggrColumn = aggrColumns[i];

          if (!_columnsLoaded.contains(aggrColumn)) {
            Dictionary dictionary = aggrFuncContext.getDictionary(i);
            double[] valueArray = _columnToValueArrayMap.get(aggrColumn);
            dictionary.readDoubleValues(_columnToDictArrayMap.get(aggrColumn), startIndex, length, valueArray,
                startIndex);
            _columnsLoaded.add(aggrColumn);
          }
        }
      }
    }
  }

  /**
   * Fetch value hashcodes for the given aggregation column and dictionary. This method is called on distinctcount
   * aggregation function.
   *
   * @param aggrColumn
   * @param dictionary
   * @param length
   */
  private void fetchColumnValueHashCodes(String aggrColumn, Dictionary dictionary, int length) {
    int[] dictIdArray = _columnToDictArrayMap.get(aggrColumn);
    if (_hashCodeArray == null) {
      _hashCodeArray = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    for (int i = 0; i < length; i++) {
      int dictId = dictIdArray[i];
      if (dictId == Dictionary.NULL_VALUE_INDEX) {
        _hashCodeArray[i] = Integer.MIN_VALUE;
      } else {
        _hashCodeArray[i] = dictionary.get(dictId).hashCode();
      }
    }
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

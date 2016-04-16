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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.utils.primitive.MutableLongValue;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction;
import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Aggregation Group By executor for multi-valued group-by columns.
 * This implementation uses MultiValueGroupKeyGenerator to generate
 * group keys, and uses Map (instead of array) to perform group by aggregation.
 *
 * The use of Map is to ensure handling of cases where cardinality product of
 * multi-valued columns can grow to limits that are not practical for array-based
 * storage.
 */
public class MultiValueGroupByExecutor implements GroupByExecutor {

  private final IndexSegment _indexSegment;
  private final List<AggregationInfo> _aggregationsInfoList;
  private final GroupBy _groupBy;

  private List<AggregationFunctionContext> _aggrFuncContextList;
  private ResultHolder[] _resultHolderArray;
  private MultiValueGroupKeyGenerator _groupKeyGenerator;
  private Map<String, int[]> _columnToDictArrayMap;
  private Map<String, double[]> _columnToValueArrayMap;

  private boolean _inited = false;
  private boolean _finished = false;

  /**
   * Constructor for the class.
   * Just stores the passed in parameters into member variables.
   * Actual initialization is performed within 'init' method.
   */
  public MultiValueGroupByExecutor(IndexSegment indexSegment, List<AggregationInfo> aggregationsInfoList,
      GroupBy groupBy) {

    _indexSegment = indexSegment;
    _aggregationsInfoList = aggregationsInfoList;
    _groupBy = groupBy;
  }

  /**
   * {@inheritDoc}
   *
   * Initializes the following:
   * - List of AggregationFunctionContexts for the given query.
   * - Group by key generator.
   * - Various re-usable arrays that store dictionaries/values/results.
   *
   */
  @Override
  public void init() {
    // Return if already initialized.
    if (_inited) {
      return;
    }
    _groupKeyGenerator = new MultiValueGroupKeyGenerator(_indexSegment, _groupBy);
    _aggrFuncContextList = new ArrayList<AggregationFunctionContext>();
    _columnToDictArrayMap = new HashMap<String, int[]>();
    _columnToValueArrayMap = new HashMap<String, double[]>();

    for (AggregationInfo aggregationInfo : _aggregationsInfoList) {
      String[] columns = aggregationInfo.getAggregationParams().get("column").trim().split(",");

      for (String column : columns) {
        if (!_columnToDictArrayMap.containsKey(column)) {
          _columnToDictArrayMap.put(column, new int[DocIdSetPlanNode.MAX_DOC_PER_CALL]);
          _columnToValueArrayMap.put(column, new double[DocIdSetPlanNode.MAX_DOC_PER_CALL]);
        }
      }
      AggregationFunctionContext aggregationFunctionContext =
          new AggregationFunctionContext(_indexSegment, aggregationInfo.getAggregationType(), columns);
      _aggrFuncContextList.add(aggregationFunctionContext);
    }

    _resultHolderArray = new ResultHolder[_aggrFuncContextList.size()];
    for (int i = 0; i < _aggrFuncContextList.size(); i++) {
      AggregationFunctionContext aggregationFunctionContext = _aggrFuncContextList.get(i);
      AggregationFunction aggregationFunction = aggregationFunctionContext.getAggregationFunction();

      // Always get map based result holder, by passing MAX_VALUE.
      _resultHolderArray[i] =
          ResultHolderFactory.getResultHolder(aggregationFunction, Integer.MAX_VALUE /* maxNumGroupKeys*/);
    }
    _inited = true;
  }

  /**
   * {@inheritDoc}
   * @param docIdSet
   * @param startIndex
   * @param length
   */
  @Override
  public void process(int[] docIdSet, int startIndex, int length) {
    Preconditions.checkState(_inited, "process cannot be called before init.");
    fetchColumnValues(docIdSet, startIndex, length);

    // Map from docId to int[] of group-keys for the docId.
    Int2ObjectOpenHashMap docIdToGroupKeys = _groupKeyGenerator.generateKeysForDocIdSet(docIdSet, startIndex, length);

    for (int i = 0; i < _aggrFuncContextList.size(); i++) {
      AggregationFunctionContext aggrFuncContext = _aggrFuncContextList.get(i);

      // For count, no need to fetch value array.
      if (aggrFuncContext.getFunctionName().equalsIgnoreCase("count")) {
        aggrFuncContext.apply(length, docIdToGroupKeys, _resultHolderArray[i], null);
      } else {
        String[] aggrColumns = aggrFuncContext.getAggregationColumns();

        for (int j = 0; j < aggrColumns.length; j++) {
          String aggrColumn = aggrColumns[j];
          double[] valueArray = _columnToValueArrayMap.get(aggrColumn);
          aggrFuncContext.apply(length, docIdToGroupKeys, _resultHolderArray[i], valueArray);
        }
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
   * {@inheritDoc}
   * @return
   */
  @Override
  public List<Map<String, Serializable>> getResult() {
    Preconditions.checkState(_finished, "GetResult cannot be called before finish.");
    List<Map<String, Serializable>> result = new ArrayList<Map<String, Serializable>>(_aggrFuncContextList.size());

    for (int i = 0; i < _aggrFuncContextList.size(); i++) {
      result.add(new HashMap<String, Serializable>());
    }

    Iterator<Pair<Long, String>> groupKeys = _groupKeyGenerator.getUniqueGroupKeys();
    while (groupKeys.hasNext()) {
      Pair<Long, String> idKeyPair = groupKeys.next();
      String stringGroupKey = idKeyPair.getSecond();

      for (int i = 0; i < _aggrFuncContextList.size(); i++) {
        AggregationFunction.ResultDataType resultDataType =
            _aggrFuncContextList.get(i).getAggregationFunction().getResultDataType();

        Serializable resultForGroupKey = getResultForKey(idKeyPair, _resultHolderArray[i], resultDataType);
        result.get(i).put(stringGroupKey, resultForGroupKey);
      }
    }
    return result;
  }

  /**
   * Returns result for the given key.
   *
   * @param idKeyPair
   * @param resultDataType
   * @return
   */
  private Serializable getResultForKey(Pair<Long, String> idKeyPair, ResultHolder resultHolder,
      AggregationFunction.ResultDataType resultDataType) {

    Serializable resultForGroupKey;
    switch (resultDataType) {
      case LONG:
        resultForGroupKey = new MutableLongValue((long) resultHolder.getDoubleResult(idKeyPair.getFirst()));
        break;

      case DOUBLE:
        resultForGroupKey = resultHolder.getDoubleResult(idKeyPair.getFirst());
        break;

      case AVERAGE_PAIR:
        Pair<Double, Long> doubleLongPair = (Pair<Double, Long>) resultHolder.getResult(idKeyPair.getFirst());
        resultForGroupKey = new AvgAggregationFunction.AvgPair(doubleLongPair.getFirst(), doubleLongPair.getSecond());
        break;

      case RANGE_PAIR:
        Pair<Double, Double> doubleDoublePair = (Pair<Double, Double>) resultHolder.getResult(idKeyPair.getFirst());
        resultForGroupKey = new MinMaxRangeAggregationFunction.MinMaxRangePair(doubleDoublePair.getFirst(),
            doubleDoublePair.getSecond());
        break;

      default:
        throw new RuntimeException("Unsupported result data type RANGE_PAIR in class " + getClass().getName());
    }
    return resultForGroupKey;
  }

  /**
   * Fetch values (dictId's) for the given docIdSet for all aggregation columns.
   *
   * @param docIdSet
   * @param startIndex
   * @param length
   */
  private void fetchColumnValues(int[] docIdSet, int startIndex, int length) {
    Set<String> columnsLoaded = new HashSet();

    for (AggregationFunctionContext aggrFuncContext : _aggrFuncContextList) {
      if (!aggrFuncContext.getFunctionName().equalsIgnoreCase("count")) {
        String[] aggrColumns = aggrFuncContext.getAggregationColumns();

        for (int i = 0; i < aggrColumns.length; i++) {
          String aggrColumn = aggrColumns[i];

          if (!columnsLoaded.contains(aggrColumn)) {
            int[] dictIdArray = _columnToDictArrayMap.get(aggrColumn);
            BlockValSet blockValSet = aggrFuncContext.getBlockValSet(i);

            blockValSet.readIntValues(docIdSet, startIndex, length, dictIdArray, startIndex);
            columnsLoaded.add(aggrColumn);

            Dictionary dictionary = aggrFuncContext.getDictionary(i);
            double[] valueArray = _columnToValueArrayMap.get(aggrColumn);
            dictionary.readDoubleValues(dictIdArray, startIndex, length, valueArray, startIndex);
          }
        }
      }
    }
  }
}

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
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
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


public class MultiValueGroupByExecutor implements GroupByExecutor {

  private final List<AggregationFunctionContext> _aggrFuncContextList;
  private final ResultHolder[] _resultHolderArray;
  private MultiValueGroupKeyGenerator _groupKeyGenerator;
  private Map<String, int[]> _columnToDictArrayMap;
  private final Map<String, double[]> _columnToValueArrayMap;
  private boolean _init = false;
  private boolean _finish = false;

  /**
   * Constructor for the class.
   */
  public MultiValueGroupByExecutor(IndexSegment indexSegment, List<AggregationInfo> aggregationsInfoList,
      GroupBy groupBy) {

    _groupKeyGenerator = new MultiValueGroupKeyGenerator(indexSegment, groupBy);
    _aggrFuncContextList = new ArrayList<AggregationFunctionContext>();
    _columnToDictArrayMap = new HashMap<String, int[]>();
    _columnToValueArrayMap = new HashMap<String, double[]>();

    for (AggregationInfo aggregationInfo : aggregationsInfoList) {
      String[] columns = aggregationInfo.getAggregationParams().get("column").trim().split(",");

      for (String column : columns) {
        if (!_columnToDictArrayMap.containsKey(column)) {
          _columnToDictArrayMap.put(column, new int[DocIdSetPlanNode.MAX_DOC_PER_CALL]);
          _columnToValueArrayMap.put(column, new double[DocIdSetPlanNode.MAX_DOC_PER_CALL]);
        }
      }
      AggregationFunctionContext aggregationFunctionContext =
          new AggregationFunctionContext(indexSegment, aggregationInfo.getAggregationType(), columns);
      _aggrFuncContextList.add(aggregationFunctionContext);
    }

    _resultHolderArray = new ResultHolder[_aggrFuncContextList.size()];
    for (int i = 0; i < _aggrFuncContextList.size(); i++) {
      double defaultValue = _aggrFuncContextList.get(i).getAggregationFunction().getDefaultValue();
      // Always get map based result holder, by passing MAX_VALUE.
      _resultHolderArray[i] = ResultHolderFactory.getResultHolder(Integer.MAX_VALUE, defaultValue);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    _init = true;
  }

  /**
   * {@inheritDoc}
   * @param docIdSet
   * @param startIndex
   * @param length
   */
  @Override
  public void process(int[] docIdSet, int startIndex, int length) {
    Preconditions.checkState(_init, "process cannot be called before init.");

    fetchColumnValues(docIdSet, startIndex, length);
    Int2ObjectOpenHashMap docIdToGroupKeys = _groupKeyGenerator.generateKeysForDocIdSet(docIdSet, startIndex, length);

    for (int i = 0; i < _aggrFuncContextList.size(); i++) {
      AggregationFunctionContext aggrFuncContext = _aggrFuncContextList.get(i);
      String[] aggrColumns = aggrFuncContext.getAggregationColumns();

      for (int j = 0; j < aggrColumns.length; j++) {
        String aggrColumn = aggrColumns[j];
        double[] valueArray = _columnToValueArrayMap.get(aggrColumn);
        aggrFuncContext.apply(length, docIdToGroupKeys, _resultHolderArray[i], valueArray);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void finish() {
    _finish = true;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public List<Map<String, Serializable>> getResult() {
    Preconditions.checkState(_finish, "GetResult cannot be called before finish.");
    List<Map<String, Serializable>> result = new ArrayList<Map<String, Serializable>>(_aggrFuncContextList.size());

    for (int i = 0; i < _aggrFuncContextList.size(); i++) {
      result.add(new HashMap<String, Serializable>());
    }

    Iterator<Pair<Long, String>> groupKeys = _groupKeyGenerator.getUniqueGroupKeys();
    while (groupKeys.hasNext()) {
      Pair<Long, String> idKeyPair = groupKeys.next();
      String stringGroupKey = idKeyPair.getSecond();

      for (int i = 0; i < _aggrFuncContextList.size(); i++) {
        double resultForGroupKey = _resultHolderArray[i].getResultForGroupKey(idKeyPair.getFirst());
        result.get(i).put(stringGroupKey, resultForGroupKey);
      }
    }
    return result;
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

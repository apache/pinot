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

import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;


/**
 * This class caches miscellaneous data to perform efficient aggregation.
 */
public class AggregationFunctionContext {
  private final AggregationFunction _aggregationFunction;

  String _aggFuncName;
  String[] _aggrColumns;

  BlockValSet[] _blockSingleValSet;
  Dictionary[] _dictionaries;

  /**
   * Constructor for the class.
   *
   * @param indexSegment
   * @param aggFuncName
   * @param columns
   */
  AggregationFunctionContext(IndexSegment indexSegment, String aggFuncName, String[] columns) {
    _aggFuncName = aggFuncName;
    _aggrColumns = columns;

    _blockSingleValSet = new BlockValSet[columns.length];
    _dictionaries = new Dictionary[columns.length];

    for (int i = 0; i < columns.length; i++) {
      DataSource dataSource = indexSegment.getDataSource(columns[i]);
      _dictionaries[i] = dataSource.getDictionary();
      _blockSingleValSet[i] = dataSource.getNextBlock().getBlockValueSet();
    }

    _aggregationFunction = AggregationFunctionFactory.getAggregationFunction(_aggFuncName);
  }

  /**
   * Returns the aggregation function object.
   * @return
   */
  public AggregationFunction getAggregationFunction() {
    return _aggregationFunction;
  }

  /**
   * Returns an array of aggregation column names.
   * @return
   */
  public String[] getAggregationColumns() {
    return _aggrColumns;
  }

  /**
   * Returns the value-set of aggregation column for the given index.
   * Assumes caller passes a valid value that can be indexed into the
   * _aggrColumns array.
   *
   * @param index
   * @return
   */
  public BlockValSet getBlockValSet(int index) {
    return _blockSingleValSet[index];
  }

  /**
   * Returns the dictionary for the column corresponding to the specified index.
   * Assumes caller passes a valid value that can be indexed into that _aggrColumns
   * array.
   *
   * @param index
   * @return
   */
  public Dictionary getDictionary(int index) {
    return _dictionaries[index];
  }

  /**
   * Apply the aggregation function for the column, given:
   * - Valid integer value that can be indexed into _aggrColumns
   * - Mapping array of docId to groupKey
   * - Array containing values for individual docIds, for the column.
   *
   * Result is populated in the passed in resultArray
   *
   * @param length
   * @param docIdToGroupKey
   * @param resultHolder
   * @param valueArray
   */
  public void apply(int length, int[] docIdToGroupKey, ResultHolder resultHolder,
      double[]... valueArray) {
    _aggregationFunction.apply(length, docIdToGroupKey, resultHolder, valueArray);
  }

  /**
   * Apply the aggregation function for the column, given
   * - Values array
   * - Group by keys each value belongs to (valueArrayIndexToGroupKeys)
   *
   * @param length
   * @param valueArrayIndexToGroupKeys
   * @param resultHolder
   * @param valueArray
   */
  public void apply(int length, Int2ObjectOpenHashMap valueArrayIndexToGroupKeys, ResultHolder resultHolder,
      double[] valueArray) {
    _aggregationFunction.apply(length, valueArrayIndexToGroupKeys, resultHolder, valueArray);
  }
}

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
import com.linkedin.pinot.core.operator.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


/**
 * This class caches miscellaneous data to perform efficient aggregation.
 */
public class AggregationFunctionContext {
  private final String _aggFuncName;
  private final AggregationFunction _aggregationFunction;
  private final String[] _aggrColumns;

  private BlockValSet[] _blockSingleValSet;
  private Dictionary[] _dictionaries;

  /**
   * Constructor for the class.
   *
   * @param indexSegment
   * @param aggFuncName
   * @param columns
   */
  public AggregationFunctionContext(IndexSegment indexSegment, String aggFuncName, String[] columns) {
    _aggFuncName = aggFuncName.toLowerCase();
    _aggregationFunction = AggregationFunctionFactory.getAggregationFunction(_aggFuncName);
    _aggrColumns = columns;

    if (_aggFuncName.equals(AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION)) {
      return;
    }

    _blockSingleValSet = new BlockValSet[columns.length];
    _dictionaries = new Dictionary[columns.length];

    for (int i = 0; i < columns.length; i++) {
      DataSource dataSource = indexSegment.getDataSource(columns[i]);
      _dictionaries[i] = dataSource.getDictionary();
      _blockSingleValSet[i] = dataSource.getNextBlock().getBlockValueSet();
    }
  }

  /**
   * Returns the aggregation function object.
   * @return
   */
  public AggregationFunction getAggregationFunction() {
    return _aggregationFunction;
  }

  /**
   * Returns the aggregation function name.
   * @return
   */
  public String getFunctionName() {
    return _aggFuncName;
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
}

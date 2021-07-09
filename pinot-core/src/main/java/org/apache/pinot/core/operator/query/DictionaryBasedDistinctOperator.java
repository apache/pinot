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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Operator which executes DISTINCT operation based on dictionary
 */
public class DictionaryBasedDistinctOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "DictionaryBasedDistinctOperator";

  private final DistinctAggregationFunction _distinctAggregationFunction;
  private final Dictionary _dictionary;
  private final int _numTotalDocs;
  private IndexSegment _indexSegment;

  private boolean _hasOrderBy;
  private boolean _isAscending;

  private int _dictLength;
  private int _numDocsScanned;

  public DictionaryBasedDistinctOperator(IndexSegment indexSegment, DistinctAggregationFunction distinctAggregationFunction,
      Dictionary dictionary, int numTotalDocs) {

    _distinctAggregationFunction = distinctAggregationFunction;
    _dictionary = dictionary;
    _numTotalDocs = numTotalDocs;

    List<OrderByExpressionContext> orderByExpressionContexts = _distinctAggregationFunction.getOrderByExpressions();

    if (orderByExpressionContexts != null) {
      OrderByExpressionContext orderByExpressionContext = orderByExpressionContexts.get(0);

      _isAscending = orderByExpressionContext.isAsc();
      _hasOrderBy = true;
    }

    _dictLength = _dictionary.length();
    _indexSegment = indexSegment;
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    DistinctTable distinctTable = buildResult();

    return new IntermediateResultsBlock(new AggregationFunction[]{_distinctAggregationFunction},
        Collections.singletonList(distinctTable), false);
  }

  /**
   * Build the final result for this operation
   */
  private DistinctTable buildResult() {

    assert _distinctAggregationFunction.getType() == AggregationFunctionType.DISTINCT;

    List<ExpressionContext> expressions = _distinctAggregationFunction.getInputExpressions();
    ExpressionContext expression = expressions.get(0);
    FieldSpec.DataType dataType = _indexSegment.getDataSource(expression.getIdentifier()).getDataSourceMetadata().getDataType();

    DataSchema dataSchema = new DataSchema(new String[]{expression.toString()},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.fromDataTypeSV(dataType)});
    List<Record> records;

    int limit = _distinctAggregationFunction.getLimit();
    int actualLimit = Math.min(limit, _dictLength);

    // If ORDER BY is not present, we read the first limit values from the dictionary and return.
    // If ORDER BY is present and the dictionary is sorted, then we read the first/last limit values
    // from the dictionary. If not sorted, then we read the entire dictionary and return it.
    if (!_hasOrderBy) {
      records = new ArrayList<>(actualLimit);

      _numDocsScanned = actualLimit;

      for (int i = 0; i < actualLimit; i++) {
        records.add(new Record(new Object[]{_dictionary.getInternal(i)}));
      }
    } else {
      if (_dictionary.isSorted()) {
        records = new ArrayList<>(actualLimit);
        if (_isAscending) {
          _numDocsScanned = actualLimit;
          for (int i = 0; i < actualLimit; i++) {
            records.add(new Record(new Object[]{_dictionary.getInternal(i)}));
          }
        } else {
          _numDocsScanned = actualLimit;
          for (int i = _dictLength - 1; i >= (_dictLength - actualLimit); i--) {
            records.add(new Record(new Object[]{_dictionary.getInternal(i)}));
          }
        }
      } else {
        DistinctTable distinctTable = new DistinctTable(dataSchema, _distinctAggregationFunction.getOrderByExpressions(), limit);

        _numDocsScanned = _dictLength;
        for (int i = 0; i < _dictLength; i++) {
          distinctTable.addWithOrderBy(new Record(new Object[]{_dictionary.getInternal(i)}));
        }

        return distinctTable;
      }
    }

    return new DistinctTable(dataSchema, records);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    // NOTE: Set numDocsScanned to numTotalDocs for backward compatibility.
    return new ExecutionStatistics(_numDocsScanned, 0, _numDocsScanned, _numTotalDocs);
  }
}

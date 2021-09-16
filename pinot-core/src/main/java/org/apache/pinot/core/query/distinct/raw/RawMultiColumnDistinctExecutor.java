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
package org.apache.pinot.core.query.distinct.raw;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.request.context.ExpressionContext;
import org.apache.pinot.spi.request.context.OrderByExpressionContext;
import org.apache.pinot.spi.utils.DataSchema;
import org.apache.pinot.spi.utils.DataSchema.ColumnDataType;


/**
 * {@link DistinctExecutor} for multiple columns where some columns are raw (non-dictionary-encoded).
 */
public class RawMultiColumnDistinctExecutor implements DistinctExecutor {
  private final List<ExpressionContext> _expressions;
  private final DistinctTable _distinctTable;

  public RawMultiColumnDistinctExecutor(List<ExpressionContext> expressions, List<DataType> dataTypes,
      @Nullable List<OrderByExpressionContext> orderByExpressions, int limit) {
    _expressions = expressions;

    int numExpressions = expressions.size();
    String[] columnNames = new String[numExpressions];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      columnNames[i] = expressions.get(i).toString();
      columnDataTypes[i] = ColumnDataType.fromDataTypeSV(dataTypes.get(i));
    }
    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
    _distinctTable = new DistinctTable(dataSchema, orderByExpressions, limit);
  }

  @Override
  public boolean process(TransformBlock transformBlock) {
    int numExpressions = _expressions.size();
    BlockValSet[] blockValSets = new BlockValSet[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      blockValSets[i] = transformBlock.getBlockValueSet(_expressions.get(i));
    }
    RowBasedBlockValueFetcher valueFetcher = new RowBasedBlockValueFetcher(blockValSets);
    int numDocs = transformBlock.getNumDocs();
    if (_distinctTable.hasOrderBy()) {
      for (int i = 0; i < numDocs; i++) {
        Record record = new Record(valueFetcher.getRow(i));
        _distinctTable.addWithOrderBy(record);
      }
    } else {
      for (int i = 0; i < numDocs; i++) {
        Record record = new Record(valueFetcher.getRow(i));
        if (_distinctTable.addWithoutOrderBy(record)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public DistinctTable getResult() {
    return _distinctTable;
  }
}

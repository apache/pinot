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
package org.apache.pinot.core.query.distinct.dictionary;

import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.distinct.BaseSingleColumnDistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.table.DictIdDistinctTable;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * {@link DistinctExecutor} for single dictionary-encoded column.
 */
public class DictionaryBasedSingleColumnDistinctExecutor
    extends BaseSingleColumnDistinctExecutor<DictIdDistinctTable, int[], int[][]> {
  private final Dictionary _dictionary;
  private final DataType _dataType;

  public DictionaryBasedSingleColumnDistinctExecutor(ExpressionContext expression, Dictionary dictionary,
      DataType dataType, int limit, boolean nullHandlingEnabled, @Nullable OrderByExpressionContext orderByExpression) {
    // NOTE: DictIdDistinctTable is created with DataSchema of actual data type, instead of INT.
    super(expression, new DictIdDistinctTable(new DataSchema(new String[]{expression.toString()},
        new ColumnDataType[]{ColumnDataType.fromDataTypeSV(dataType)}), limit, nullHandlingEnabled, orderByExpression));
    _dictionary = dictionary;
    _dataType = dataType;
  }

  @Override
  protected int[] getValuesSV(BlockValSet blockValSet) {
    return blockValSet.getDictionaryIdsSV();
  }

  @Override
  protected int[][] getValuesMV(BlockValSet blockValSet) {
    return blockValSet.getDictionaryIdsMV();
  }

  @Override
  protected boolean processSV(int[] dictIds, int from, int to) {
    if (_distinctTable.hasLimit()) {
      if (_distinctTable.hasOrderBy()) {
        for (int i = from; i < to; i++) {
          _distinctTable.addWithOrderBy(dictIds[i]);
        }
      } else {
        for (int i = from; i < to; i++) {
          if (_distinctTable.addWithoutOrderBy(dictIds[i])) {
            return true;
          }
        }
      }
    } else {
      for (int i = from; i < to; i++) {
        _distinctTable.addUnbounded(dictIds[i]);
      }
    }
    return false;
  }

  @Override
  protected boolean processMV(int[][] dictIds, int from, int to) {
    if (_distinctTable.hasLimit()) {
      if (_distinctTable.hasOrderBy()) {
        for (int i = from; i < to; i++) {
          for (int dictId : dictIds[i]) {
            _distinctTable.addWithOrderBy(dictId);
          }
        }
      } else {
        for (int i = from; i < to; i++) {
          for (int dictId : dictIds[i]) {
            if (_distinctTable.addWithoutOrderBy(dictId)) {
              return true;
            }
          }
        }
      }
    } else {
      for (int i = from; i < to; i++) {
        for (int dictId : dictIds[i]) {
          _distinctTable.addUnbounded(dictId);
        }
      }
    }
    return false;
  }

  @Override
  public DistinctTable getResult() {
    return _distinctTable.toTypedDistinctTable(_dictionary, _distinctTable.hasNull());
  }
}

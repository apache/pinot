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

import it.unimi.dsi.fastutil.ints.IntIterator;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.distinct.BaseSingleColumnDistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.table.BigDecimalDistinctTable;
import org.apache.pinot.core.query.distinct.table.BytesDistinctTable;
import org.apache.pinot.core.query.distinct.table.DictIdDistinctTable;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.core.query.distinct.table.DoubleDistinctTable;
import org.apache.pinot.core.query.distinct.table.FloatDistinctTable;
import org.apache.pinot.core.query.distinct.table.IntDistinctTable;
import org.apache.pinot.core.query.distinct.table.LongDistinctTable;
import org.apache.pinot.core.query.distinct.table.StringDistinctTable;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


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
    DataSchema dataSchema = _distinctTable.getDataSchema();
    int limit = _distinctTable.getLimit();
    boolean nullHandlingEnabled = _distinctTable.isNullHandlingEnabled();
    OrderByExpressionContext orderByExpression = _distinctTable.getOrderByExpression();
    IntIterator dictIdIterator = _distinctTable.getValueSet().iterator();
    boolean hasNull = _distinctTable.hasNull();
    switch (_dictionary.getValueType()) {
      case INT: {
        IntDistinctTable distinctTable =
            new IntDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
        while (dictIdIterator.hasNext()) {
          distinctTable.addUnbounded(_dictionary.getIntValue(dictIdIterator.nextInt()));
        }
        if (hasNull) {
          distinctTable.addNull();
        }
        return distinctTable;
      }
      case LONG: {
        LongDistinctTable distinctTable =
            new LongDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
        while (dictIdIterator.hasNext()) {
          distinctTable.addUnbounded(_dictionary.getLongValue(dictIdIterator.nextInt()));
        }
        if (hasNull) {
          distinctTable.addNull();
        }
        return distinctTable;
      }
      case FLOAT: {
        FloatDistinctTable distinctTable =
            new FloatDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
        while (dictIdIterator.hasNext()) {
          distinctTable.addUnbounded(_dictionary.getFloatValue(dictIdIterator.nextInt()));
        }
        if (hasNull) {
          distinctTable.addNull();
        }
        return distinctTable;
      }
      case DOUBLE: {
        DoubleDistinctTable distinctTable =
            new DoubleDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
        while (dictIdIterator.hasNext()) {
          distinctTable.addUnbounded(_dictionary.getDoubleValue(dictIdIterator.nextInt()));
        }
        if (hasNull) {
          distinctTable.addNull();
        }
        return distinctTable;
      }
      case BIG_DECIMAL: {
        BigDecimalDistinctTable distinctTable =
            new BigDecimalDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
        while (dictIdIterator.hasNext()) {
          distinctTable.addUnbounded(_dictionary.getBigDecimalValue(dictIdIterator.nextInt()));
        }
        if (hasNull) {
          distinctTable.addNull();
        }
        return distinctTable;
      }
      case STRING: {
        StringDistinctTable distinctTable =
            new StringDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
        while (dictIdIterator.hasNext()) {
          distinctTable.addUnbounded(_dictionary.getStringValue(dictIdIterator.nextInt()));
        }
        if (hasNull) {
          distinctTable.addNull();
        }
        return distinctTable;
      }
      case BYTES: {
        BytesDistinctTable distinctTable =
            new BytesDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
        while (dictIdIterator.hasNext()) {
          distinctTable.addUnbounded(new ByteArray(_dictionary.getBytesValue(dictIdIterator.nextInt())));
        }
        if (hasNull) {
          distinctTable.addNull();
        }
        return distinctTable;
      }
      default:
        throw new IllegalStateException("Unsupported data type: " + _dataType);
    }
  }
}

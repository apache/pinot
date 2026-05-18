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
package org.apache.pinot.core.query.distinct.table;

import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.utils.ByteArray;


public class DictIdDistinctTable extends IntDistinctTable {

  public DictIdDistinctTable(DataSchema dataSchema, int limit, boolean nullHandlingEnabled,
      @Nullable OrderByExpressionContext orderByExpression) {
    super(dataSchema, limit, nullHandlingEnabled, orderByExpression);
  }

  public IntOpenHashSet getValueSet() {
    return _valueSet;
  }

  @Nullable
  public OrderByExpressionContext getOrderByExpression() {
    return _orderByExpression;
  }

  @Override
  protected IntComparator getComparator(OrderByExpressionContext orderByExpression) {
    return orderByExpression.isAsc() ? (v1, v2) -> Integer.compare(v2, v1)
        : (v1, v2) -> Integer.compare(v1, v2);
  }

  /**
   * Adds a dictId while iterating in final ORDER BY order.
   *
   * <p>When nulls sort first, the null placeholder consumes one top-N slot. For nulls last, the top-N rows are
   * determined entirely by non-null values, so early termination should not reserve space for null.
   */
  public boolean addForOrderedEarlyTermination(int dictId) {
    assert _orderByExpression != null;
    assert hasLimit();
    _valueSet.add(dictId);
    int targetSize = _hasNull && !_orderByExpression.isNullsLast() ? _limitWithoutNull : _limit;
    return _valueSet.size() >= targetSize;
  }

  @Override
  public void mergeDistinctTable(DistinctTable distinctTable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean mergeDataTable(DataTable dataTable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Object[]> getRows() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataTable toDataTable()
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultTable toResultTable() {
    throw new UnsupportedOperationException();
  }

  /**
   * Converts this dictId-based table to a typed distinct table by resolving dictionary values.
   */
  public DistinctTable toTypedDistinctTable(Dictionary dictionary, boolean hasNull) {
    DataSchema dataSchema = getDataSchema();
    int limit = getLimit();
    boolean nullHandlingEnabled = isNullHandlingEnabled();
    OrderByExpressionContext orderByExpression = getOrderByExpression();
    IntIterator dictIdIterator = _valueSet.iterator();
    switch (dictionary.getValueType()) {
      case INT: {
        IntDistinctTable table = new IntDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
        if (hasNull) {
          table.addNull();
        }
        while (dictIdIterator.hasNext()) {
          table.addUnbounded(dictionary.getIntValue(dictIdIterator.nextInt()));
        }
        return table;
      }
      case LONG: {
        LongDistinctTable table = new LongDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
        if (hasNull) {
          table.addNull();
        }
        while (dictIdIterator.hasNext()) {
          table.addUnbounded(dictionary.getLongValue(dictIdIterator.nextInt()));
        }
        return table;
      }
      case FLOAT: {
        FloatDistinctTable table = new FloatDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
        if (hasNull) {
          table.addNull();
        }
        while (dictIdIterator.hasNext()) {
          table.addUnbounded(dictionary.getFloatValue(dictIdIterator.nextInt()));
        }
        return table;
      }
      case DOUBLE: {
        DoubleDistinctTable table = new DoubleDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
        if (hasNull) {
          table.addNull();
        }
        while (dictIdIterator.hasNext()) {
          table.addUnbounded(dictionary.getDoubleValue(dictIdIterator.nextInt()));
        }
        return table;
      }
      case BIG_DECIMAL: {
        BigDecimalDistinctTable table =
            new BigDecimalDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
        if (hasNull) {
          table.addNull();
        }
        while (dictIdIterator.hasNext()) {
          table.addUnbounded(dictionary.getBigDecimalValue(dictIdIterator.nextInt()));
        }
        return table;
      }
      case STRING: {
        StringDistinctTable table = new StringDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
        if (hasNull) {
          table.addNull();
        }
        while (dictIdIterator.hasNext()) {
          table.addUnbounded(dictionary.getStringValue(dictIdIterator.nextInt()));
        }
        return table;
      }
      case BYTES: {
        BytesDistinctTable table = new BytesDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpression);
        if (hasNull) {
          table.addNull();
        }
        while (dictIdIterator.hasNext()) {
          table.addUnbounded(new ByteArray(dictionary.getBytesValue(dictIdIterator.nextInt())));
        }
        return table;
      }
      default:
        throw new IllegalStateException("Unsupported data type: " + dictionary.getValueType());
    }
  }
}

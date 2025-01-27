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
package org.apache.pinot.core.query.distinct;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.query.distinct.dictionary.DictionaryBasedMultiColumnDistinctExecutor;
import org.apache.pinot.core.query.distinct.dictionary.DictionaryBasedSingleColumnDistinctExecutor;
import org.apache.pinot.core.query.distinct.raw.BigDecimalDistinctExecutor;
import org.apache.pinot.core.query.distinct.raw.BytesDistinctExecutor;
import org.apache.pinot.core.query.distinct.raw.DoubleDistinctExecutor;
import org.apache.pinot.core.query.distinct.raw.FloatDistinctExecutor;
import org.apache.pinot.core.query.distinct.raw.IntDistinctExecutor;
import org.apache.pinot.core.query.distinct.raw.LongDistinctExecutor;
import org.apache.pinot.core.query.distinct.raw.RawMultiColumnDistinctExecutor;
import org.apache.pinot.core.query.distinct.raw.StringDistinctExecutor;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Factory for {@link DistinctExecutor}.
 */
public class DistinctExecutorFactory {
  private DistinctExecutorFactory() {
  }

  /**
   * Returns the {@link DistinctExecutor} for the given distinct query.
   */
  public static DistinctExecutor getDistinctExecutor(BaseProjectOperator<?> projectOperator,
      QueryContext queryContext) {
    List<ExpressionContext> expressions = queryContext.getSelectExpressions();
    int limit = queryContext.getLimit();
    boolean nullHandlingEnabled = queryContext.isNullHandlingEnabled();
    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 1) {
      // Single column
      ExpressionContext expression = expressions.get(0);
      ColumnContext columnContext = projectOperator.getResultColumnContext(expression);
      DataType dataType = columnContext.getDataType();
      OrderByExpressionContext orderByExpression;
      if (orderByExpressions != null) {
        assert orderByExpressions.size() == 1;
        orderByExpression = orderByExpressions.get(0);
        assert orderByExpression.getExpression().equals(expression);
      } else {
        orderByExpression = null;
      }
      Dictionary dictionary = columnContext.getDictionary();
      // Note: Use raw value based when ordering is needed and dictionary is not sorted (consuming segments).
      if (dictionary != null && (orderByExpression == null || dictionary.isSorted())) {
        // Dictionary based
        return new DictionaryBasedSingleColumnDistinctExecutor(expression, dictionary, dataType, limit,
            nullHandlingEnabled, orderByExpression);
      } else {
        // Raw value based
        switch (dataType.getStoredType()) {
          case INT:
            return new IntDistinctExecutor(expression, dataType, limit, nullHandlingEnabled, orderByExpression);
          case LONG:
            return new LongDistinctExecutor(expression, dataType, limit, nullHandlingEnabled, orderByExpression);
          case FLOAT:
            return new FloatDistinctExecutor(expression, dataType, limit, nullHandlingEnabled, orderByExpression);
          case DOUBLE:
            return new DoubleDistinctExecutor(expression, dataType, limit, nullHandlingEnabled, orderByExpression);
          case BIG_DECIMAL:
            return new BigDecimalDistinctExecutor(expression, dataType, limit, nullHandlingEnabled, orderByExpression);
          case STRING:
            return new StringDistinctExecutor(expression, dataType, limit, nullHandlingEnabled, orderByExpression);
          case BYTES:
            return new BytesDistinctExecutor(expression, dataType, limit, nullHandlingEnabled, orderByExpression);
          default:
            throw new IllegalStateException("Unsupported data type: " + dataType);
        }
      }
    } else {
      // Multiple columns
      boolean hasMVExpression = false;
      String[] columnNames = new String[numExpressions];
      ColumnDataType[] columnDataTypes = new ColumnDataType[numExpressions];
      List<Dictionary> dictionaries = new ArrayList<>(numExpressions);
      boolean dictionaryBased = true;
      for (int i = 0; i < numExpressions; i++) {
        ExpressionContext expression = expressions.get(i);
        ColumnContext columnContext = projectOperator.getResultColumnContext(expression);
        if (!columnContext.isSingleValue()) {
          hasMVExpression = true;
        }
        columnNames[i] = expression.toString();
        columnDataTypes[i] = ColumnDataType.fromDataTypeSV(columnContext.getDataType());
        if (dictionaryBased) {
          Dictionary dictionary = columnContext.getDictionary();
          if (dictionary != null) {
            dictionaries.add(dictionary);
          } else {
            dictionaryBased = false;
          }
        }
      }
      DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
      // Note: Use raw value based when ordering is needed and dictionary is not sorted (consuming segments).
      if (dictionaryBased && orderByExpressions != null) {
        for (OrderByExpressionContext orderByExpression : orderByExpressions) {
          int index = ArrayUtils.indexOf(columnNames, orderByExpression.getExpression().toString());
          assert index >= 0;
          if (!dictionaries.get(index).isSorted()) {
            dictionaryBased = false;
            break;
          }
        }
      }
      if (dictionaryBased) {
        // Dictionary based
        return new DictionaryBasedMultiColumnDistinctExecutor(expressions, hasMVExpression, dataSchema, dictionaries,
            limit, nullHandlingEnabled, orderByExpressions);
      } else {
        // Raw value based
        return new RawMultiColumnDistinctExecutor(expressions, hasMVExpression, dataSchema, limit, nullHandlingEnabled,
            orderByExpressions);
      }
    }
  }
}

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
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.distinct.dictionary.DictionaryBasedMultiColumnDistinctOnlyExecutor;
import org.apache.pinot.core.query.distinct.dictionary.DictionaryBasedMultiColumnDistinctOrderByExecutor;
import org.apache.pinot.core.query.distinct.dictionary.DictionaryBasedSingleColumnDistinctOnlyExecutor;
import org.apache.pinot.core.query.distinct.dictionary.DictionaryBasedSingleColumnDistinctOrderByExecutor;
import org.apache.pinot.core.query.distinct.raw.RawBytesSingleColumnDistinctOnlyExecutor;
import org.apache.pinot.core.query.distinct.raw.RawBytesSingleColumnDistinctOrderByExecutor;
import org.apache.pinot.core.query.distinct.raw.RawDoubleSingleColumnDistinctOnlyExecutor;
import org.apache.pinot.core.query.distinct.raw.RawDoubleSingleColumnDistinctOrderByExecutor;
import org.apache.pinot.core.query.distinct.raw.RawFloatSingleColumnDistinctOnlyExecutor;
import org.apache.pinot.core.query.distinct.raw.RawFloatSingleColumnDistinctOrderByExecutor;
import org.apache.pinot.core.query.distinct.raw.RawIntSingleColumnDistinctOnlyExecutor;
import org.apache.pinot.core.query.distinct.raw.RawIntSingleColumnDistinctOrderByExecutor;
import org.apache.pinot.core.query.distinct.raw.RawLongSingleColumnDistinctOnlyExecutor;
import org.apache.pinot.core.query.distinct.raw.RawLongSingleColumnDistinctOrderByExecutor;
import org.apache.pinot.core.query.distinct.raw.RawMultiColumnDistinctExecutor;
import org.apache.pinot.core.query.distinct.raw.RawStringSingleColumnDistinctOnlyExecutor;
import org.apache.pinot.core.query.distinct.raw.RawStringSingleColumnDistinctOrderByExecutor;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.OrderByExpressionContext;
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
  public static DistinctExecutor getDistinctExecutor(DistinctAggregationFunction distinctAggregationFunction,
      TransformOperator transformOperator) {
    List<ExpressionContext> expressions = distinctAggregationFunction.getInputExpressions();
    List<OrderByExpressionContext> orderByExpressions = distinctAggregationFunction.getOrderByExpressions();
    int limit = distinctAggregationFunction.getLimit();
    if (orderByExpressions == null) {
      return getDistinctOnlyExecutor(expressions, limit, transformOperator);
    } else {
      return getDistinctOrderByExecutor(expressions, orderByExpressions, limit, transformOperator);
    }
  }

  private static DistinctExecutor getDistinctOnlyExecutor(List<ExpressionContext> expressions, int limit,
      TransformOperator transformOperator) {
    if (expressions.size() == 1) {
      // Single column
      ExpressionContext expression = expressions.get(0);
      Dictionary dictionary = transformOperator.getDictionary(expression);
      if (dictionary != null) {
        // Dictionary based
        return new DictionaryBasedSingleColumnDistinctOnlyExecutor(expression, dictionary, limit);
      } else {
        // Raw value based
        DataType dataType = transformOperator.getResultMetadata(expression).getDataType();
        switch (dataType) {
          case INT:
            return new RawIntSingleColumnDistinctOnlyExecutor(expression, limit);
          case LONG:
            return new RawLongSingleColumnDistinctOnlyExecutor(expression, limit);
          case FLOAT:
            return new RawFloatSingleColumnDistinctOnlyExecutor(expression, limit);
          case DOUBLE:
            return new RawDoubleSingleColumnDistinctOnlyExecutor(expression, limit);
          case STRING:
            return new RawStringSingleColumnDistinctOnlyExecutor(expression, limit);
          case BYTES:
            return new RawBytesSingleColumnDistinctOnlyExecutor(expression, limit);
          default:
            throw new IllegalStateException();
        }
      }
    } else {
      // Multiple columns
      int numExpressions = expressions.size();
      List<Dictionary> dictionaries = new ArrayList<>(numExpressions);
      boolean dictionaryBased = true;
      for (ExpressionContext expression : expressions) {
        Dictionary dictionary = transformOperator.getDictionary(expression);
        if (dictionary != null) {
          dictionaries.add(dictionary);
        } else {
          dictionaryBased = false;
          break;
        }
      }
      if (dictionaryBased) {
        // Dictionary based
        return new DictionaryBasedMultiColumnDistinctOnlyExecutor(expressions, dictionaries, limit);
      } else {
        // Raw value based
        List<DataType> dataTypes = new ArrayList<>(numExpressions);
        for (ExpressionContext expression : expressions) {
          dataTypes.add(transformOperator.getResultMetadata(expression).getDataType());
        }
        return new RawMultiColumnDistinctExecutor(expressions, dataTypes, null, limit);
      }
    }
  }

  private static DistinctExecutor getDistinctOrderByExecutor(List<ExpressionContext> expressions,
      List<OrderByExpressionContext> orderByExpressions, int limit, TransformOperator transformOperator) {
    if (expressions.size() == 1) {
      // Single column
      ExpressionContext expression = expressions.get(0);
      Dictionary dictionary = transformOperator.getDictionary(expression);
      // Note: Use raw value based when dictionary is not sorted (consuming segments).
      if (dictionary != null && dictionary.isSorted()) {
        // Dictionary based
        assert orderByExpressions.size() == 1;
        return new DictionaryBasedSingleColumnDistinctOrderByExecutor(expression, dictionary, orderByExpressions.get(0),
            limit);
      } else {
        // Raw value based
        DataType dataType = transformOperator.getResultMetadata(expression).getDataType();
        assert orderByExpressions.size() == 1;
        OrderByExpressionContext orderByExpression = orderByExpressions.get(0);
        switch (dataType) {
          case INT:
            return new RawIntSingleColumnDistinctOrderByExecutor(expression, orderByExpression, limit);
          case LONG:
            return new RawLongSingleColumnDistinctOrderByExecutor(expression, orderByExpression, limit);
          case FLOAT:
            return new RawFloatSingleColumnDistinctOrderByExecutor(expression, orderByExpression, limit);
          case DOUBLE:
            return new RawDoubleSingleColumnDistinctOrderByExecutor(expression, orderByExpression, limit);
          case STRING:
            return new RawStringSingleColumnDistinctOrderByExecutor(expression, orderByExpression, limit);
          case BYTES:
            return new RawBytesSingleColumnDistinctOrderByExecutor(expression, orderByExpression, limit);
          default:
            throw new IllegalStateException();
        }
      }
    } else {
      // Multiple columns
      int numExpressions = expressions.size();
      List<Dictionary> dictionaries = new ArrayList<>(numExpressions);
      boolean dictionaryBased = true;
      for (ExpressionContext expression : expressions) {
        Dictionary dictionary = transformOperator.getDictionary(expression);
        // Note: Use raw value based when dictionary is not sorted (consuming segments).
        if (dictionary != null && dictionary.isSorted()) {
          dictionaries.add(dictionary);
        } else {
          dictionaryBased = false;
          break;
        }
      }
      if (dictionaryBased) {
        // Dictionary based
        return new DictionaryBasedMultiColumnDistinctOrderByExecutor(expressions, dictionaries, orderByExpressions,
            limit);
      } else {
        // Raw value based
        List<DataType> dataTypes = new ArrayList<>(numExpressions);
        for (ExpressionContext expression : expressions) {
          dataTypes.add(transformOperator.getResultMetadata(expression).getDataType());
        }
        return new RawMultiColumnDistinctExecutor(expressions, dataTypes, orderByExpressions, limit);
      }
    }
  }
}

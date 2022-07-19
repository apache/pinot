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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.distinct.dictionary.DictionaryBasedMultiColumnDistinctOnlyExecutor;
import org.apache.pinot.core.query.distinct.dictionary.DictionaryBasedMultiColumnDistinctOrderByExecutor;
import org.apache.pinot.core.query.distinct.dictionary.DictionaryBasedSingleColumnDistinctOnlyExecutor;
import org.apache.pinot.core.query.distinct.dictionary.DictionaryBasedSingleColumnDistinctOrderByExecutor;
import org.apache.pinot.core.query.distinct.raw.RawBigDecimalSingleColumnDistinctOnlyExecutor;
import org.apache.pinot.core.query.distinct.raw.RawBigDecimalSingleColumnDistinctOrderByExecutor;
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
      TransformOperator transformOperator, boolean nullHandlingEnabled) {
    List<ExpressionContext> expressions = distinctAggregationFunction.getInputExpressions();
    List<OrderByExpressionContext> orderByExpressions = distinctAggregationFunction.getOrderByExpressions();
    int limit = distinctAggregationFunction.getLimit();
    if (orderByExpressions == null) {
      return getDistinctOnlyExecutor(expressions, limit, transformOperator, nullHandlingEnabled);
    } else {
      return getDistinctOrderByExecutor(
          expressions, orderByExpressions, limit, transformOperator, nullHandlingEnabled);
    }
  }

  private static DistinctExecutor getDistinctOnlyExecutor(List<ExpressionContext> expressions, int limit,
      TransformOperator transformOperator, boolean nullHandlingEnabled) {
    int numExpressions = expressions.size();
    if (numExpressions == 1) {
      // Single column
      ExpressionContext expression = expressions.get(0);
      TransformResultMetadata expressionMetadata = transformOperator.getResultMetadata(expression);
      DataType dataType = expressionMetadata.getDataType();
      Dictionary dictionary = transformOperator.getDictionary(expression);
      if (dictionary != null && !nullHandlingEnabled) {
        // Dictionary based
        return new DictionaryBasedSingleColumnDistinctOnlyExecutor(expression, dictionary, dataType, limit);
      } else {
        // Raw value based
        switch (dataType.getStoredType()) {
          case INT:
            return new RawIntSingleColumnDistinctOnlyExecutor(expression, dataType, limit, nullHandlingEnabled);
          case LONG:
            return new RawLongSingleColumnDistinctOnlyExecutor(expression, dataType, limit, nullHandlingEnabled);
          case FLOAT:
            return new RawFloatSingleColumnDistinctOnlyExecutor(expression, dataType, limit, nullHandlingEnabled);
          case DOUBLE:
            return new RawDoubleSingleColumnDistinctOnlyExecutor(expression, dataType, limit, nullHandlingEnabled);
          case BIG_DECIMAL:
            return new RawBigDecimalSingleColumnDistinctOnlyExecutor(expression, dataType, limit, nullHandlingEnabled);
          case STRING:
            return new RawStringSingleColumnDistinctOnlyExecutor(expression, dataType, limit, nullHandlingEnabled);
          case BYTES:
            return new RawBytesSingleColumnDistinctOnlyExecutor(expression, dataType, limit, nullHandlingEnabled);
          default:
            throw new IllegalStateException();
        }
      }
    } else {
      // Multiple columns
      boolean hasMVExpression = false;
      List<DataType> dataTypes = new ArrayList<>(numExpressions);
      for (ExpressionContext expression : expressions) {
        TransformResultMetadata expressionMetadata = transformOperator.getResultMetadata(expression);
        if (!expressionMetadata.isSingleValue()) {
          hasMVExpression = true;
        }
        dataTypes.add(expressionMetadata.getDataType());
      }
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
        return new DictionaryBasedMultiColumnDistinctOnlyExecutor(expressions, hasMVExpression, dictionaries, dataTypes,
            limit);
      } else {
        // Raw value based
        return new RawMultiColumnDistinctExecutor(expressions, hasMVExpression, dataTypes, null, limit);
      }
    }
  }

  private static DistinctExecutor getDistinctOrderByExecutor(List<ExpressionContext> expressions,
      List<OrderByExpressionContext> orderByExpressions, int limit, TransformOperator transformOperator,
      boolean nullHandlingEnabled) {
    int numExpressions = expressions.size();
    if (numExpressions == 1) {
      // Single column
      ExpressionContext expression = expressions.get(0);
      TransformResultMetadata expressionMetadata = transformOperator.getResultMetadata(expression);
      DataType dataType = expressionMetadata.getDataType();
      assert orderByExpressions.size() == 1;
      OrderByExpressionContext orderByExpression = orderByExpressions.get(0);
      Dictionary dictionary = transformOperator.getDictionary(expression);
      // Note: Use raw value based when dictionary is not sorted (consuming segments).
      if (dictionary != null && dictionary.isSorted() && !nullHandlingEnabled) {
        // Dictionary based
        return new DictionaryBasedSingleColumnDistinctOrderByExecutor(expression, dictionary, dataType,
            orderByExpressions.get(0), limit);
      } else {
        // Raw value based
        switch (dataType.getStoredType()) {
          case INT:
            return new RawIntSingleColumnDistinctOrderByExecutor(expression, dataType, orderByExpression, limit,
                nullHandlingEnabled);
          case LONG:
            return new RawLongSingleColumnDistinctOrderByExecutor(expression, dataType, orderByExpression, limit,
                nullHandlingEnabled);
          case FLOAT:
            return new RawFloatSingleColumnDistinctOrderByExecutor(expression, dataType, orderByExpression, limit,
                nullHandlingEnabled);
          case DOUBLE:
            return new RawDoubleSingleColumnDistinctOrderByExecutor(expression, dataType, orderByExpression, limit,
                nullHandlingEnabled);
          case BIG_DECIMAL:
            return new RawBigDecimalSingleColumnDistinctOrderByExecutor(expression, dataType, orderByExpression, limit,
                nullHandlingEnabled);
          case STRING:
            return new RawStringSingleColumnDistinctOrderByExecutor(expression, dataType, orderByExpression, limit,
                nullHandlingEnabled);
          case BYTES:
            return new RawBytesSingleColumnDistinctOrderByExecutor(expression, dataType, orderByExpression, limit,
                nullHandlingEnabled);
          default:
            throw new IllegalStateException();
        }
      }
    } else {
      // Multiple columns
      boolean hasMVExpression = false;
      List<DataType> dataTypes = new ArrayList<>(numExpressions);
      for (ExpressionContext expression : expressions) {
        TransformResultMetadata expressionMetadata = transformOperator.getResultMetadata(expression);
        if (!expressionMetadata.isSingleValue()) {
          hasMVExpression = true;
        }
        dataTypes.add(expressionMetadata.getDataType());
      }
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
        return new DictionaryBasedMultiColumnDistinctOrderByExecutor(expressions, hasMVExpression, dictionaries,
            dataTypes, orderByExpressions, limit);
      } else {
        // Raw value based
        return new RawMultiColumnDistinctExecutor(expressions, hasMVExpression, dataTypes, orderByExpressions, limit);
      }
    }
  }
}

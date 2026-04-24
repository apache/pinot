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
package org.apache.pinot.segment.local.recordtransformer;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.ThrottledLogger;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared evaluation of function expressions during ingestion. Two call sites exist for historical reasons:
 * <ul>
 *   <li>{@link ExpressionTransformer} &mdash; {@link org.apache.pinot.spi.recordtransformer.RecordTransformer} chain.
 *   Table/schema {@link org.apache.pinot.spi.config.table.ingestion.TransformConfig}s, topological order,
 *   null / overwrite / implicit MAP rules, and {@code continueOnError} (see
 *   {@link #applyExpressionTransformations})</li>
 *   <li>{@link org.apache.pinot.segment.local.recordtransformer.enricher.function.CustomFunctionEnricher} &mdash;
 *   {@link org.apache.pinot.spi.recordtransformer.enricher.RecordEnricher} with JSON
 *   {@code fieldToFunctionMap}; see {@link #applyEnricherEvaluations}.</li>
 * </ul>
 * Configuration, ordering, and pipeline wiring stay in the respective classes.
 */
public final class IngestionFunctionEvaluation {
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestionFunctionEvaluation.class);

  private IngestionFunctionEvaluation() {
  }

  /**
   * Enricher: each target field is set to {@code evaluate(record)} (map iteration order; segment-local evaluators).
   */
  public static void applyEnricherEvaluations(GenericRow record,
      Map<String, org.apache.pinot.segment.local.function.FunctionEvaluator> fieldToFunctionEvaluator) {
    fieldToFunctionEvaluator.forEach(
        (field, evaluator) -> record.putValue(field, evaluator.evaluate(record)));
  }

  /**
   * {@link ExpressionTransformer#transform} semantics: SPI {@link FunctionEvaluator} map, post-upsert overwrite flag,
   * and implicit MAP-derived column handling.
   */
  public static void applyExpressionTransformations(GenericRow record,
      Map<String, FunctionEvaluator> expressionEvaluators, boolean continueOnError, boolean overwriteExistingValues,
      Set<String> implicitMapTransformColumns, ThrottledLogger throttledLogger) {
    for (Map.Entry<String, FunctionEvaluator> entry : expressionEvaluators.entrySet()) {
      String column = entry.getKey();
      FunctionEvaluator transformFunctionEvaluator = entry.getValue();
      Object existingValue = record.getValue(column);
      boolean shouldApplyTransform =
          overwriteExistingValues || existingValue == null || record.isNullValue(column);
      if (shouldApplyTransform) {
        try {
          Object transformedValue = transformFunctionEvaluator.evaluate(record);
          applyTransformedValue(record, column, transformedValue);
        } catch (Exception e) {
          if (!continueOnError) {
            throw new RuntimeException("Caught exception while evaluation transform function for column: " + column, e);
          }
          throttledLogger.warn("Caught exception while evaluation transform function for column: " + column, e);
          record.markIncomplete();
        }
      } else if (existingValue.getClass().isArray() || existingValue instanceof Collection
          || existingValue instanceof Map) {
        try {
          Object transformedValue = transformFunctionEvaluator.evaluate(record);
          if (transformedValue == null && implicitMapTransformColumns.contains(column)) {
            continue;
          }
          if (!isTypeCompatible(existingValue, transformedValue)) {
            applyTransformedValue(record, column, transformedValue);
          }
        } catch (Exception e) {
          LOGGER.debug("Caught exception while evaluation transform function for column: {}", column, e);
        }
      }
    }
  }

  private static void applyTransformedValue(GenericRow record, String column, @Nullable Object transformedValue) {
    if (transformedValue != null) {
      record.removeNullValueField(column);
      record.putValue(column, transformedValue);
    } else {
      record.removeValue(column);
      record.addNullValueField(column);
    }
  }

  private static boolean isTypeCompatible(Object existingValue, @Nullable Object transformedValue) {
    if (transformedValue == null || existingValue == null) {
      return transformedValue == existingValue;
    }
    if (transformedValue.getClass() == existingValue.getClass()) {
      return true;
    }
    if (transformedValue instanceof Collection && existingValue instanceof Collection) {
      return true;
    }
    if (transformedValue instanceof Map && existingValue instanceof Map) {
      return true;
    }
    return transformedValue.getClass().isArray() && existingValue.getClass().isArray();
  }
}

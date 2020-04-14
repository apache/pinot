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
package org.apache.pinot.spi.data.function.evaluators;

import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.utils.SchemaFieldExtractorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory class to create an {@link ExpressionEvaluator} for the field spec based on the {@link FieldSpec#getTransformFunction()}
 */
public class ExpressionEvaluatorFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExpressionEvaluatorFactory.class);

  private ExpressionEvaluatorFactory() {

  }

  /**
   * Creates the {@link ExpressionEvaluator} for the given field spec
   *
   * 1. If transform expression is defined, use it to create {@link ExpressionEvaluator}
   * 2. For TIME column, {@link DefaultTimeSpecEvaluator} for backward compatible handling of time spec. This is needed until we migrate to {@link org.apache.pinot.spi.data.DateTimeFieldSpec}
   * 3. For columns ending with __KEYS or __VALUES (used for interpreting Map column in Avro), create default functions for handing the Map
   * 4. Return null, if none of the above
   */
  @Nullable
  public static ExpressionEvaluator getExpressionEvaluator(FieldSpec fieldSpec) {
    ExpressionEvaluator expressionEvaluator = null;

    String columnName = fieldSpec.getName();
    String transformExpression = fieldSpec.getTransformFunction();
    if (transformExpression != null) {

      // if transform function expression present, use it to generate function evaluator
      try {
        expressionEvaluator = getExpressionEvaluator(transformExpression);
      } catch (Exception e) {
        LOGGER.error(
            "Caught exception while constructing expression evaluator for transform expression: {}, of column: {}, skipping",
            transformExpression, columnName, e);
      }
    } else if (fieldSpec.getFieldType().equals(FieldSpec.FieldType.TIME)) {

      // for backward compatible handling of TIME filed conversion
      expressionEvaluator = new DefaultTimeSpecEvaluator((TimeFieldSpec) fieldSpec);
    } else if (columnName.endsWith(SchemaFieldExtractorUtils.MAP_KEY_COLUMN_SUFFIX)) {

      // for backward compatible handling of Map type (currently only in Avro)
      String sourceMapName =
          columnName.substring(0, columnName.length() - SchemaFieldExtractorUtils.MAP_KEY_COLUMN_SUFFIX.length());
      String defaultMapKeysTransformExpression = getDefaultMapKeysTransformExpression(sourceMapName);
      expressionEvaluator = getExpressionEvaluator(defaultMapKeysTransformExpression);
    } else if (columnName.endsWith(SchemaFieldExtractorUtils.MAP_VALUE_COLUMN_SUFFIX)) {

      // for backward compatible handling of Map type in avro (currently only in Avro)
      String sourceMapName =
          columnName.substring(0, columnName.length() - SchemaFieldExtractorUtils.MAP_VALUE_COLUMN_SUFFIX.length());
      String defaultMapValuesTransformExpression = getDefaultMapValuesTransformExpression(sourceMapName);
      expressionEvaluator = getExpressionEvaluator(defaultMapValuesTransformExpression);
    }

    return expressionEvaluator;
  }

  private static ExpressionEvaluator getExpressionEvaluator(String transformExpression) {
    ExpressionEvaluator expressionEvaluator = null;
    if (transformExpression != null && !transformExpression.isEmpty()) {
      if (transformExpression.startsWith(GroovyExpressionEvaluator.getGroovyExpressionPrefix())) {
        expressionEvaluator = new GroovyExpressionEvaluator(transformExpression);
      }
    }
    return expressionEvaluator;
  }

  private static String getDefaultMapKeysTransformExpression(String mapColumnName) {
    return String.format("Groovy({%s.sort()*.key}, %s)", mapColumnName, mapColumnName);
  }

  private static String getDefaultMapValuesTransformExpression(String mapColumnName) {
    return String.format("Groovy({%s.sort()*.value}, %s)", mapColumnName, mapColumnName);
  }
}

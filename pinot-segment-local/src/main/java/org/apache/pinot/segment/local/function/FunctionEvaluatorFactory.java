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
package org.apache.pinot.segment.local.function;

import javax.annotation.Nullable;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;


/**
 * Factory class to create an {@link FunctionEvaluator} for the field spec based on the {@link FieldSpec#getTransformFunction()}
 */
public class FunctionEvaluatorFactory {
  private FunctionEvaluatorFactory() {
  }

  /**
   * Creates the {@link FunctionEvaluator} for the given field spec
   *
   * 1. If transform expression is defined, use it to create the appropriate {@link FunctionEvaluator}
   * 2. For TIME column, if conversion is needed, {@link TimeSpecFunctionEvaluator} for backward compatible handling of time spec. This
   * is needed until we migrate to {@link org.apache.pinot.spi.data.DateTimeFieldSpec}
   * 3. For columns ending with __KEYS or __VALUES (used for interpreting Map column in Avro), create default groovy functions for
   * handing the Map
   * 4. Return null, if none of the above
   */
  @Nullable
  public static FunctionEvaluator getExpressionEvaluator(FieldSpec fieldSpec) {
    FunctionEvaluator functionEvaluator = null;

    String columnName = fieldSpec.getName();
    // TODO: once we have published a release w/ IngestionConfig#TransformConfigs, stop reading transform function from schema in next
    //  release
    String transformExpression = fieldSpec.getTransformFunction();
    if (transformExpression != null && !transformExpression.isEmpty()) {

      // if transform function expression present, use it to generate function evaluator
      try {
        functionEvaluator = getExpressionEvaluator(transformExpression);
      } catch (Exception e) {
        throw new IllegalStateException(
            "Caught exception while constructing expression evaluator for transform expression:" + transformExpression + ", of column:"
                + columnName);
      }
    } else if (fieldSpec.getFieldType().equals(FieldSpec.FieldType.TIME)) {

      // Time conversions should be done using DateTimeFieldSpec and transformFunctions
      // But we need below lines for converting TimeFieldSpec's incoming to outgoing
      TimeFieldSpec timeFieldSpec = (TimeFieldSpec) fieldSpec;
      TimeGranularitySpec incomingGranularitySpec = timeFieldSpec.getIncomingGranularitySpec();
      TimeGranularitySpec outgoingGranularitySpec = timeFieldSpec.getOutgoingGranularitySpec();
      if (!incomingGranularitySpec.equals(outgoingGranularitySpec)) {
        if (!incomingGranularitySpec.getName().equals(outgoingGranularitySpec.getName())) {
          functionEvaluator = new TimeSpecFunctionEvaluator(incomingGranularitySpec, outgoingGranularitySpec);
        } else {
          throw new IllegalStateException(
              "Invalid timeSpec - Incoming and outgoing field specs are different, but name " + incomingGranularitySpec.getName()
                  + " is same");
        }
      }
    } else if (columnName.endsWith(SchemaUtils.MAP_KEY_COLUMN_SUFFIX)) {

      // for backward compatible handling of Map type (currently only in Avro)
      String sourceMapName = columnName.substring(0, columnName.length() - SchemaUtils.MAP_KEY_COLUMN_SUFFIX.length());
      String defaultMapKeysTransformExpression = getDefaultMapKeysTransformExpression(sourceMapName);
      functionEvaluator = getExpressionEvaluator(defaultMapKeysTransformExpression);
    } else if (columnName.endsWith(SchemaUtils.MAP_VALUE_COLUMN_SUFFIX)) {
      // for backward compatible handling of Map type in avro (currently only in Avro)
      String sourceMapName = columnName.substring(0, columnName.length() - SchemaUtils.MAP_VALUE_COLUMN_SUFFIX.length());
      String defaultMapValuesTransformExpression = getDefaultMapValuesTransformExpression(sourceMapName);
      functionEvaluator = getExpressionEvaluator(defaultMapValuesTransformExpression);
    }
    return functionEvaluator;
  }

  public static FunctionEvaluator getExpressionEvaluator(String transformExpression) {
    if (transformExpression.startsWith(GroovyFunctionEvaluator.getGroovyExpressionPrefix())) {
      return new GroovyFunctionEvaluator(transformExpression);
    } else {
      return new InbuiltFunctionEvaluator(transformExpression);
    }
  }

  private static String getDefaultMapKeysTransformExpression(String mapColumnName) {
    return String.format("Groovy({%s.sort()*.key}, %s)", mapColumnName, mapColumnName);
  }

  private static String getDefaultMapValuesTransformExpression(String mapColumnName) {
    return String.format("Groovy({%s.sort()*.value}, %s)", mapColumnName, mapColumnName);
  }
}

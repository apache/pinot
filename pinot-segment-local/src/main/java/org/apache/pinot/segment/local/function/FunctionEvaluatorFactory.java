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
 * Deprecated factory shim that preserves the historical evaluator entry points under
 * {@code org.apache.pinot.segment.local.function}.
 *
 * <p>This class is stateless and thread-safe.
 *
 * @deprecated Use {@link org.apache.pinot.common.evaluator.FunctionEvaluatorFactory} instead.
 */
@Deprecated
public class FunctionEvaluatorFactory {
  private FunctionEvaluatorFactory() {
  }

  @Nullable
  public static FunctionEvaluator getExpressionEvaluator(FieldSpec fieldSpec) {
    FunctionEvaluator functionEvaluator = null;

    String columnName = fieldSpec.getName();
    String transformExpression = fieldSpec.getTransformFunction();
    if (transformExpression != null && !transformExpression.isEmpty()) {
      try {
        functionEvaluator = getExpressionEvaluator(transformExpression);
      } catch (Exception e) {
        throw new IllegalStateException(
            "Caught exception while constructing expression evaluator for transform expression: " + transformExpression
                + " of column: " + columnName + ", exception: " + e.getMessage(), e);
      }
    } else if (fieldSpec.getFieldType() == FieldSpec.FieldType.TIME) {
      TimeFieldSpec timeFieldSpec = (TimeFieldSpec) fieldSpec;
      TimeGranularitySpec incomingGranularitySpec = timeFieldSpec.getIncomingGranularitySpec();
      TimeGranularitySpec outgoingGranularitySpec = timeFieldSpec.getOutgoingGranularitySpec();
      if (!incomingGranularitySpec.equals(outgoingGranularitySpec)) {
        if (!incomingGranularitySpec.getName().equals(outgoingGranularitySpec.getName())) {
          functionEvaluator = new TimeSpecFunctionEvaluator(incomingGranularitySpec, outgoingGranularitySpec);
        } else {
          throw new IllegalStateException(
              "Invalid timeSpec - Incoming and outgoing field specs are different, but name " + incomingGranularitySpec
                  .getName() + " is same");
        }
      }
    } else if (columnName.endsWith(SchemaUtils.MAP_KEY_COLUMN_SUFFIX)) {
      String sourceMapName = columnName.substring(0, columnName.length() - SchemaUtils.MAP_KEY_COLUMN_SUFFIX.length());
      functionEvaluator = getExpressionEvaluator(getDefaultMapKeysTransformExpression(sourceMapName));
    } else if (columnName.endsWith(SchemaUtils.MAP_VALUE_COLUMN_SUFFIX)) {
      String sourceMapName =
          columnName.substring(0, columnName.length() - SchemaUtils.MAP_VALUE_COLUMN_SUFFIX.length());
      functionEvaluator = getExpressionEvaluator(getDefaultMapValuesTransformExpression(sourceMapName));
    }
    return functionEvaluator;
  }

  public static FunctionEvaluator getExpressionEvaluator(String transformExpression) {
    if (isGroovyExpression(transformExpression)) {
      return new GroovyFunctionEvaluator(transformExpression);
    } else {
      return new InbuiltFunctionEvaluator(transformExpression);
    }
  }

  public static boolean isGroovyExpression(String transformExpression) {
    return transformExpression.startsWith(GroovyFunctionEvaluator.getGroovyExpressionPrefix());
  }

  private static String getDefaultMapKeysTransformExpression(String mapColumnName) {
    return String.format("Groovy({%s.sort()*.key}, %s)", mapColumnName, mapColumnName);
  }

  private static String getDefaultMapValuesTransformExpression(String mapColumnName) {
    return String.format("Groovy({%s.sort()*.value}, %s)", mapColumnName, mapColumnName);
  }
}

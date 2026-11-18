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
package org.apache.pinot.common.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;


/// Factory class to create an [FunctionEvaluator] for the field spec based on the
/// [FieldSpec#getTransformFunction()]
public class FunctionEvaluatorFactory {
  private static final String MAP_KEY_COLUMN_SUFFIX = "__KEYS";
  private static final String MAP_VALUE_COLUMN_SUFFIX = "__VALUES";

  private FunctionEvaluatorFactory() {
  }

  /// Creates the [FunctionEvaluator] for the given field spec
  ///
  /// 1. If transform expression is defined, use it to create the appropriate [FunctionEvaluator]
  /// 2. For TIME column, if conversion is needed, [TimeSpecFunctionEvaluator] for backward compatible handling
  /// of time spec. This
  /// is needed until we migrate to [org.apache.pinot.spi.data.DateTimeFieldSpec]
  /// 3. For columns ending with \_\_KEYS or \_\_VALUES (used for interpreting Map columns in Avro), extract
  /// keys or values in key order without compiling Groovy
  /// 4. Return null, if none of the above
  @Nullable
  public static FunctionEvaluator getExpressionEvaluator(FieldSpec fieldSpec) {
    FunctionEvaluator functionEvaluator = null;

    String columnName = fieldSpec.getName();
    // TODO: once we have published a release w/ IngestionConfig#TransformConfigs, stop reading transform function
    //  from schema in next
    //  release
    String transformExpression = fieldSpec.getTransformFunction();
    if (transformExpression != null && !transformExpression.isEmpty()) {

      // if transform function expression present, use it to generate function evaluator
      try {
        functionEvaluator = getExpressionEvaluator(transformExpression);
      } catch (Exception e) {
        throw new IllegalStateException(
            "Caught exception while constructing expression evaluator for transform expression: " + transformExpression
                + " of column: " + columnName + ", exception: " + e.getMessage(), e);
      }
    } else if (fieldSpec.getFieldType() == FieldSpec.FieldType.TIME) {

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
              "Invalid timeSpec - Incoming and outgoing field specs are different, but name " + incomingGranularitySpec
                  .getName() + " is same");
        }
      }
    } else if (columnName.endsWith(MAP_KEY_COLUMN_SUFFIX)) {

      // for backward compatible handling of Map type (currently only in Avro)
      String sourceMapName = columnName.substring(0, columnName.length() - MAP_KEY_COLUMN_SUFFIX.length());
      functionEvaluator = new MapFunctionEvaluator(sourceMapName, true);
    } else if (columnName.endsWith(MAP_VALUE_COLUMN_SUFFIX)) {
      // for backward compatible handling of Map type in avro (currently only in Avro)
      String sourceMapName =
          columnName.substring(0, columnName.length() - MAP_VALUE_COLUMN_SUFFIX.length());
      functionEvaluator = new MapFunctionEvaluator(sourceMapName, false);
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

  /// @return true if the given transform function is a groovy expression, otherwise returns false
  public static boolean isGroovyExpression(String transformExpression) {
    return transformExpression.startsWith(GroovyFunctionEvaluator.getGroovyExpressionPrefix());
  }

  /// Extracts legacy Avro map keys or values in key order. Immutable and safe to share between threads.
  private static class MapFunctionEvaluator implements FunctionEvaluator {
    private final String _mapColumnName;
    private final boolean _extractKeys;

    MapFunctionEvaluator(String mapColumnName, boolean extractKeys) {
      _mapColumnName = mapColumnName;
      _extractKeys = extractKeys;
    }

    @Override
    public List<String> getArguments() {
      return List.of(_mapColumnName);
    }

    @Override
    public Object evaluate(GenericRow genericRow) {
      return evaluateMap(genericRow.getValue(_mapColumnName));
    }

    @Override
    public Object evaluate(Object[] values) {
      return evaluateMap(values[0]);
    }

    @Nullable
    private List<?> evaluateMap(@Nullable Object value) {
      if (value == null) {
        return null;
      }
      Map<?, ?> map = (Map<?, ?>) value;
      // Groovy's sort() preserves the comparator of an existing SortedMap.
      Map<?, ?> sortedMap = map instanceof SortedMap<?, ?> ? map : new TreeMap<>(map);
      return new ArrayList<>(_extractKeys ? sortedMap.keySet() : sortedMap.values());
    }
  }
}
